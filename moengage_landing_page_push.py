import gspread
import pandas as pd
import requests
import base64
import time
import os
import sys
import json

# Configure stdout to use UTF-8 to avoid encoding errors on Windows
sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Load .env file locally if it exists
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split("=", 1)
                    if len(parts) == 2:
                        os.environ[parts[0].strip()] = parts[1].strip()

load_env()

# ==============================
# CONFIGURATION
# ==============================
credentials_path = 'D:/Downloads/db-mismath-starship-data-ff9d8efaf2bb.json'
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

SOURCE_SHEET_NAME = "Ambassador Club - Landing Page Responses"
SOURCE_TAB_NAME = "Sheet1"

DEST_TAB_NAME = "Moengage"

workspace_id = os.environ.get("MOENGAGE_WORKSPACE_ID")
api_key = os.environ.get("MOENGAGE_API_KEY")

if not workspace_id or not api_key:
    print("❌ ERROR: MOENGAGE_WORKSPACE_ID and MOENGAGE_API_KEY must be set in environment or .env file.")
    sys.exit(1)

data_center = "01"

BATCH_SIZE = 500
MAX_WORKERS = 20
RETRY_LIMIT = 3
RETRY_DELAY = 2

# Pause after every 2000 uploads
UPLOAD_CHUNK_SIZE = 2000

# Daily upload limit
DAILY_LIMIT = 2000

# When true, never prompt for input — just run straight through all chunks.
NON_INTERACTIVE = os.environ.get("NON_INTERACTIVE", "true").lower() == "true"

# When true, do everything except actually POST to MoEngage / write
# uploads to the sheet.
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

# ==============================
# COLUMN NAMES
# ==============================
SRC_PHONE_COL = "mobile"
DEST_PHONE_COL = "Phone Number"
SOURCE_DATE_COL = "date"

# ==============================
# MOENGAGE SETUP
# ==============================
url = f"https://api-{data_center}.moengage.com/v1/customer/{workspace_id}?app_id={workspace_id}"

auth_string = f"{workspace_id}:{api_key}"
auth_encoded = base64.b64encode(auth_string.encode()).decode()

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {auth_encoded}"
}

# ==============================
# GOOGLE SHEETS SETUP
# ==============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

if GOOGLE_SERVICE_ACCOUNT_JSON:
    key_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        key_dict,
        scope
    )
else:
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        credentials_path,
        scope
    )

client = gspread.authorize(creds)

try:
    spreadsheet = client.open(SOURCE_SHEET_NAME)
except Exception as e:
    print(f"❌ Failed to open spreadsheet '{SOURCE_SHEET_NAME}': {e}")
    sys.exit(1)

try:
    source_sheet = spreadsheet.worksheet(SOURCE_TAB_NAME)
except Exception as e:
    print(f"❌ Failed to open worksheet '{SOURCE_TAB_NAME}': {e}")
    sys.exit(1)

source_data = pd.DataFrame(
    source_sheet.get_all_records()
)

source_data.columns = source_data.columns.str.strip()

print(f"📊 Total rows loaded from source: {len(source_data)}")
print(f"📌 Source columns: {list(source_data.columns)}")

# ==============================
# DESTINATION SHEET
# ==============================
try:
    dest_sheet = spreadsheet.worksheet(DEST_TAB_NAME)
except gspread.WorksheetNotFound:
    dest_sheet = spreadsheet.add_worksheet(
        title=DEST_TAB_NAME,
        rows="100000",
        cols="20"
    )

# ==============================
# HEADER SETUP
# ==============================
if not dest_sheet.get_all_values():
    dest_sheet.append_row([
        "City",
        "Society name",
        "Name",
        "E-mail",
        "Phone Number",
        "Month",
        "Date",
        "Remarks"
    ])

# ==============================
# THREAD LOCK
# ==============================
sheet_lock = Lock()

# ==============================
# BUILD SET OF UPLOADED PHONES
# ==============================
uploaded_phones = set()

all_values = dest_sheet.get_all_values()

if len(all_values) > 1:
    headers_row = [str(h).strip() for h in all_values[0]]
    try:
        phone_col_index = headers_row.index(DEST_PHONE_COL)
        for row in all_values[1:]:
            if len(row) > phone_col_index:
                raw_phone = str(row[phone_col_index]).strip()
                digits = ''.join(filter(str.isdigit, raw_phone))
                if len(digits) >= 10:
                    uploaded_phones.add(digits[-10:])
    except ValueError:
        print(f"❌ '{DEST_PHONE_COL}' column not found in dest tab")

print(f"📋 Already uploaded numbers found: {len(uploaded_phones)}")

# ==============================
# HELPERS
# ==============================
def clean_phone(phone):
    digits = ''.join(filter(str.isdigit, str(phone)))
    if len(digits) >= 10:
        return digits[-10:]
    return None

def extract_month(date_str):
    try:
        cleaned = str(date_str).split(" GMT")[0]
        dt = datetime.strptime(cleaned, "%a %b %d %Y %H:%M:%S")
        return dt.strftime("%B")
    except:
        return ""

def get_row_status(row):
    phone = clean_phone(row.get(SRC_PHONE_COL, ""))
    if not phone:
        return "invalid_phone"
    if phone in uploaded_phones:
        return "already_uploaded"
    return "eligible"

def build_payload(row):
    phone_last10 = clean_phone(row.get(SRC_PHONE_COL, ""))
    phone_with_code = "+91" + phone_last10
    name = str(row.get("name", "")).strip()
    parts = name.split()
    first_name = parts[0] if parts else ""
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""

    now = datetime.now()

    attributes = {
        "first_name": first_name,
        "last_name": last_name,
        "mobile": phone_with_code,
        "email": str(row.get("email", "")).strip(),
        "city": str(row.get("city", "")).strip(),
        "society_name": str(row.get("society_name", "")).strip(),
        "occupation": str(row.get("occupation", "")).strip(),
        "group_type": "Ambassador Club Meta Leads",
        "upload_time": int(time.time()),
        "upload_date": now.strftime("%Y-%m-%d"),
        "upload_time_readable": now.strftime("%Y-%m-%d %H:%M:%S")
    }

    payload = {
        "type": "customer",
        "customer_id": phone_with_code,
        "attributes": attributes
    }

    return payload, phone_last10

def push_to_moengage(row):
    payload, phone_last10 = build_payload(row)
    if DRY_RUN:
        return (row, phone_last10, True, None)

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return (row, phone_last10, True, None)
        except Exception as e:
            if attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
            else:
                return (row, phone_last10, False, str(e))

    return (row, phone_last10, False, "Failed after retries")

def flush_to_sheet(rows_with_remarks):
    if not rows_with_remarks:
        return
    if DRY_RUN:
        print(f"🧪 [DRY RUN] Would log {len(rows_with_remarks)} rows to sheet")
        return

    today_str = datetime.now().strftime("%d-%b-%Y")
    data = []
    for row, remarks in rows_with_remarks:
        phone_raw = row.get(SRC_PHONE_COL, "")
        data.append([
            row.get("city", ""),
            row.get("society_name", ""),
            str(row.get("name", "")).strip(),
            str(row.get("email", "")).strip(),
            str(phone_raw),
            extract_month(row.get(SOURCE_DATE_COL, "")),
            today_str,
            remarks
        ])

    for attempt in range(RETRY_LIMIT):
        try:
            with sheet_lock:
                dest_sheet.append_rows(
                    data,
                    value_input_option="RAW",
                    table_range="A:H"
                )
            return
        except Exception as e:
            if attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ Sheet write failed: {e}")

# ==============================
# SPLIT ROWS
# ==============================
eligible_rows = []
already_uploaded = []
invalid_rows = []

for _, row in source_data.iterrows():
    status = get_row_status(row)
    if status == "eligible":
        eligible_rows.append(row)
    elif status == "already_uploaded":
        already_uploaded.append(row)
    else:
        invalid_rows.append(row)

# Enforce daily limit of 2000 numbers
original_eligible_count = len(eligible_rows)
if original_eligible_count > DAILY_LIMIT:
    print(f"⚠️ Limiting eligible rows from {original_eligible_count} to {DAILY_LIMIT} (daily limit requirement)")
    eligible_rows = eligible_rows[:DAILY_LIMIT]

print(f"\n🟢 Eligible to upload      : {len(eligible_rows)}")
print(f"🔵 Already uploaded        : {len(already_uploaded)}")
print(f"⚠️ Invalid phone rows      : {len(invalid_rows)}")

# ==============================
# LOG ALREADY-UPLOADED
# ==============================
if already_uploaded:
    flush_to_sheet([
        (row, "Already in Moengage tab")
        for row in already_uploaded
    ])
    print(f"📝 Logged {len(already_uploaded)} duplicate rows")

# ==============================
# PROCESS IN CHUNKS
# ==============================
total_uploaded = 0
total_failed = 0

for chunk_start in range(0, len(eligible_rows), UPLOAD_CHUNK_SIZE):
    chunk_end = chunk_start + UPLOAD_CHUNK_SIZE
    current_chunk = eligible_rows[chunk_start:chunk_end]

    print("\n" + "=" * 60)
    print(f"🚀 Processing rows {chunk_start + 1} to {min(chunk_end, len(eligible_rows))}")
    print("=" * 60)

    success_batch = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(push_to_moengage, row)
            for row in current_chunk
        ]

        for future in as_completed(futures):
            row, phone, success, error = future.result()
            if success:
                uploaded_phones.add(phone)
                success_batch.append((row, "Uploaded"))
                total_uploaded += 1
                print(f"✅ Uploaded: +91{phone} ({total_uploaded})")
            else:
                success_batch.append((row, f"Upload Failed: {error}"))
                total_failed += 1
                print(f"❌ Failed: +91{phone} — {error}")

            # Flush every batch
            if len(success_batch) >= BATCH_SIZE:
                flush_to_sheet(success_batch)
                success_batch = []
                time.sleep(1)

    # Final flush
    if success_batch:
        flush_to_sheet(success_batch)

    # Ask user before next chunk (skipped entirely in CI/non-interactive runs)
    if chunk_end < len(eligible_rows) and not NON_INTERACTIVE:
        user_input = input(f"\n➡️ Continue next {UPLOAD_CHUNK_SIZE} rows? (yes/no): ").strip().lower()
        if user_input not in ["yes", "y"]:
            print("\n⛔ Upload stopped by user.")
            break

# ==============================
# SUMMARY
# ==============================
print("\n" + "=" * 50)
print(f"🟢 Eligible rows found      : {len(eligible_rows)}")
print(f"✅ Successfully uploaded    : {total_uploaded}")
print(f"❌ Upload failed            : {total_failed}")
print(f"🔵 Already uploaded         : {len(already_uploaded)}")
print(f"⚠️ Invalid phone rows       : {len(invalid_rows)}")
print("=" * 50)
