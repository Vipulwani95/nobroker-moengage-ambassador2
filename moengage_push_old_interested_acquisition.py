import gspread
import requests
import base64
import json
import time
import os
import sys
# Configure stdout to use UTF-8 to avoid encoding errors on Windows
sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime, timedelta
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

SOURCE_SHEET_NAME = "Acquisition Campaigns and Calling Ambassador"
SOURCE_TAB_NAMES = [
    "FB Campaign_Acquisition_Jan25",
    "FB Campaign_Acquisition_Nov25",
    "FB Campaign_Acquisition_Nov23",
    "Society Manager_Acquisition"
]

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

UPLOAD_CHUNK_SIZE = 1000

# When true, never prompt for input — just run straight through all chunks.
NON_INTERACTIVE = os.environ.get("NON_INTERACTIVE", "true").lower() == "true"

# When true, do everything except actually POST to MoEngage / write
# uploads to the sheet.
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

GROUP_TYPE = "Ambassador - Interested but form not filled"

# ==============================
# COLUMN INDEX MAP (0-indexed)
# A=0 B=1 C=2 D=3 E=4 F=5 G=6 H=7 I=8 J=9 K=10
# ... O=14 ... AG=32
# ==============================
COL_CITY = 3       # D
COL_SOCIETY = 5    # F
COL_NAME = 6       # G
COL_EMAIL = 7      # H
COL_PHONE = 9      # J
COL_STATUS = 10    # K -> must contain "Interested"
COL_DATE = 14      # O -> date column
COL_DASH = 32      # AG -> must contain "-"

HEADER_ROWS = 1  # number of header rows to skip in source sheet

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

# Open spreadsheet
try:
    spreadsheet = client.open(SOURCE_SHEET_NAME)
except Exception as e:
    print(f"❌ Failed to open spreadsheet '{SOURCE_SHEET_NAME}': {e}")
    sys.exit(1)

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
DEST_PHONE_COL = "Phone Number"

all_dest_values = dest_sheet.get_all_values()

if len(all_dest_values) > 1:
    headers_row = [str(h).strip() for h in all_dest_values[0]]
    try:
        phone_col_index = headers_row.index(DEST_PHONE_COL)
        for row in all_dest_values[1:]:
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
def get_cell(row, idx):
    if idx < len(row):
        return str(row[idx]).strip()
    return ""

def clean_phone(phone):
    digits = ''.join(filter(str.isdigit, str(phone)))
    if len(digits) >= 10:
        return digits[-10:]
    return None

def get_ist_now():
    # GitHub Actions environment runs in UTC, so we add 5.5 hours to get IST
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def parse_sheet_date(date_str):
    # Try different formats
    formats_to_try = ["%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"]
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None

def is_date_in_range(date_val_str):
    parsed = parse_sheet_date(date_val_str)
    if not parsed:
        return False
    ist_today = get_ist_now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = ist_today - timedelta(days=5)
    return start_date <= parsed <= ist_today

def row_matches_filter(row, tab_name):
    status = get_cell(row, COL_STATUS).lower()
    status_matched = ("interested" in status) or \
                     ("want's call back later" in status) or \
                     ("wants call back later" in status) or \
                     ("call back later" in status)

    if not status_matched:
        return False

    date_val = get_cell(row, COL_DATE)
    if not is_date_in_range(date_val):
        return False

    col_dash = 29 if tab_name == "Society Manager_Acquisition" else 32
    dash_val = get_cell(row, col_dash)
    if "-" not in dash_val:
        return False

    return True

def get_row_status(row, tab_name):
    if not row_matches_filter(row, tab_name):
        return "not_matching"

    phone = clean_phone(get_cell(row, COL_PHONE))
    if not phone:
        return "invalid_phone"

    if phone in uploaded_phones:
        return "already_uploaded"

    return "eligible"

def build_payload(row):
    phone_last10 = clean_phone(get_cell(row, COL_PHONE))
    phone_with_code = "+91" + phone_last10
    name = get_cell(row, COL_NAME)
    parts = name.split()
    first_name = parts[0] if parts else ""
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""

    now = datetime.now()

    attributes = {
        "first_name": first_name,
        "last_name": last_name,
        "mobile": phone_with_code,
        "email": get_cell(row, COL_EMAIL),
        "city": get_cell(row, COL_CITY),
        "society_name": get_cell(row, COL_SOCIETY),
        "group_type": GROUP_TYPE,
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

    today_display = datetime.now().strftime("%d-%b-%Y")
    data = []
    for row, remarks in rows_with_remarks:
        data.append([
            get_cell(row, COL_CITY),
            get_cell(row, COL_SOCIETY),
            get_cell(row, COL_NAME),
            get_cell(row, COL_EMAIL),
            get_cell(row, COL_PHONE),
            today_display,
            remarks
        ])

    for attempt in range(RETRY_LIMIT):
        try:
            with sheet_lock:
                dest_sheet.append_rows(
                    data,
                    value_input_option="RAW",
                    table_range="A:G"
                )
            return
        except Exception as e:
            if attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ Sheet write failed: {e}")

# ==============================
# LOAD AND PROCESS ALL SOURCE TABS
# ==============================
all_source_values = []
for tab_name in SOURCE_TAB_NAMES:
    try:
        source_sheet = spreadsheet.worksheet(tab_name)
        print(f"📖 Loading rows from sheet '{tab_name}'...")
        sheet_values = source_sheet.get_all_values()
        if len(sheet_values) > HEADER_ROWS:
            for row in sheet_values[HEADER_ROWS:]:
                all_source_values.append((row, tab_name))
            print(f"✅ Loaded {len(sheet_values) - HEADER_ROWS} rows from '{tab_name}'")
        else:
            print(f"⚠️ Sheet '{tab_name}' is empty or only contains headers.")
    except gspread.WorksheetNotFound:
        print(f"❌ Worksheet '{tab_name}' not found. Skipping...")
    except Exception as e:
        print(f"❌ Error loading worksheet '{tab_name}': {e}. Skipping...")

print(f"📊 Total raw rows loaded from all sources: {len(all_source_values)}")
print(f"🔧 Mode: {'DRY RUN' if DRY_RUN else 'LIVE'} | {'Non-interactive' if NON_INTERACTIVE else 'Interactive'}")

# ==============================
# SPLIT ROWS
# ==============================
eligible_rows = []
already_uploaded_rows = []
invalid_rows = []
skipped_rows = 0

for row, tab_name in all_source_values:
    status = get_row_status(row, tab_name)
    if status == "eligible":
        eligible_rows.append(row)
    elif status == "already_uploaded":
        already_uploaded_rows.append(row)
    elif status == "invalid_phone":
        invalid_rows.append(row)
    else:
        skipped_rows += 1

print(f"\n🟢 Eligible to upload      : {len(eligible_rows)}")
print(f"🔵 Already uploaded        : {len(already_uploaded_rows)}")
print(f"⚠️ Invalid phone rows      : {len(invalid_rows)}")
print(f"⏭️ Skipped (filter no match): {skipped_rows}")

# ==============================
# LOG ALREADY-UPLOADED
# ==============================
if already_uploaded_rows:
    flush_to_sheet([
        (row, "Already in Moengage tab")
        for row in already_uploaded_rows
    ])
    print(f"📝 Logged {len(already_uploaded_rows)} duplicate rows")

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
        user_input = input("\n➡️ Continue next 1000 rows? (yes/no): ").strip().lower()
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
print(f"🔵 Already uploaded         : {len(already_uploaded_rows)}")
print(f"⚠️ Invalid phone rows       : {len(invalid_rows)}")
print(f"⏭️ Skipped (filter no match) : {skipped_rows}")
print("=" * 50)
