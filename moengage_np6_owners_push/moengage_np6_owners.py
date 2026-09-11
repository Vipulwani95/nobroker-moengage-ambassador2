import os
import sys
import re
import json
import time
import base64
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor, as_completed

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

current_month_str = datetime.now().strftime("%b-%Y")
source_sheet_name = os.environ.get("NP6_SHEET_NAME", current_month_str)
source_worksheet_name = os.environ.get("NP6_SHEET_NAME", current_month_str)
destination_sheet_name = "Moengage"

workspace_id = os.environ.get("MOENGAGE_WORKSPACE_ID")
api_key = os.environ.get("MOENGAGE_API_KEY")

if not workspace_id or not api_key:
    print("❌ ERROR: MOENGAGE_WORKSPACE_ID and MOENGAGE_API_KEY must be set in environment or .env file.")
    sys.exit(1)

data_center = "01"

BATCH_SIZE = 100          # Rows written to Google Sheet at once
MAX_WORKERS = 20          # Concurrent MoEngage API calls
RETRY_LIMIT = 3           # Retries per failed API call
RETRY_DELAY = 2           # Seconds between retries

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
TEST_LIMIT = os.environ.get("TEST_LIMIT")
if TEST_LIMIT:
    try:
        TEST_LIMIT = int(TEST_LIMIT)
        print(f"⚠️ Running in TEST mode. Limit: {TEST_LIMIT}")
    except ValueError:
        TEST_LIMIT = None

NEEDED_SOURCE_FIELDS = [
    "ownerPhone", "rejectReason", "ownerName", "submitterPhone",
    "creationDate", "incomingState", "city", "currentState",
    "amountPaid", "paidOn", "transactionId", "listingId", "subState",
]

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
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
else:
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)

client = gspread.authorize(creds)

source_sheet = client.open(source_sheet_name).worksheet(source_worksheet_name)

try:
    dest_sheet = client.open(source_sheet_name).worksheet(destination_sheet_name)
except gspread.WorksheetNotFound:
    dest_sheet = client.open(source_sheet_name).add_worksheet(
        title=destination_sheet_name, rows="100000", cols="20"
    )

def col_letter(idx):
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def fetch_selected_columns(sheet, header, field_names, start_row=2):
    col_index = {}
    missing = []
    for name in field_names:
        if name in header:
            col_index[name] = header.index(name) + 1
        else:
            missing.append(name)
    if missing:
        raise ValueError(f"Column(s) not found in sheet header: {missing}")

    ranges = [f"{col_letter(col_index[name])}{start_row}:{col_letter(col_index[name])}"
              for name in field_names]
    results = sheet.batch_get(ranges)

    flattened = []
    for col_values in results:
        flattened.append([row[0] if row else "" for row in col_values])

    max_len = max((len(c) for c in flattened), default=0)
    padded = [c + [""] * (max_len - len(c)) for c in flattened]

    return {name: padded[i] for i, name in enumerate(field_names)}, max_len

source_header = source_sheet.row_values(1)
source_columns_data, total_rows_len = fetch_selected_columns(source_sheet, source_header, NEEDED_SOURCE_FIELDS)

source_rows = [
    {field: source_columns_data[field][i] for field in NEEDED_SOURCE_FIELDS}
    for i in range(total_rows_len)
]

print(f"📊 Total source rows loaded: {len(source_rows)}")
print(f"🔧 Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")

dest_header = dest_sheet.row_values(1)

if not dest_header:
    dest_cols = NEEDED_SOURCE_FIELDS + ["Remarks"]
    dest_sheet.update(range_name="A1", values=[dest_cols], value_input_option="RAW")
    uploaded_phones = set()
    next_row = 2
else:
    dest_cols = dest_header
    if "Remarks" not in dest_cols:
        dest_cols.append("Remarks")

    uploaded_phones = set()
    if "ownerPhone" in dest_header:
        phone_col_idx = dest_header.index("ownerPhone") + 1
        phone_range = f"{col_letter(phone_col_idx)}2:{col_letter(phone_col_idx)}"
        phone_result = dest_sheet.batch_get([phone_range])
        phone_values = [row[0] if row else "" for row in phone_result[0]] if phone_result else []
        for ph in phone_values:
            ph = str(ph).strip()
            if ph:
                uploaded_phones.add(ph[-10:])
        next_row = len(phone_values) + 2
    else:
        next_row = 2

print(f"📍 Destination header: {dest_cols}")
print(f"📍 Next free row to write: {next_row}")
print(f"📋 Already uploaded phones: {len(uploaded_phones)}")

today_str = datetime.now().strftime("%d-%b-%Y")

def is_valid_row(row):
    owner_phone = str(row.get("ownerPhone", "")).strip()
    np_status = str(row.get("rejectReason", "")).strip()
    if not owner_phone or np_status != "NP6":
        return False
    if owner_phone[-10:] in uploaded_phones:
        return False
    return True

pending_rows = [row for row in source_rows if is_valid_row(row)]
print(f"🚀 Rows matching NP6 & eligible: {len(pending_rows)}")

if TEST_LIMIT and len(pending_rows) > TEST_LIMIT:
    pending_rows = pending_rows[:TEST_LIMIT]
    print(f"⚠️ Truncated pending rows to TEST_LIMIT ({TEST_LIMIT})")

def build_payload(row):
    owner_phone = str(row.get("ownerPhone", "")).strip()
    owner_phone_last10 = owner_phone[-10:]
    phone_with_code = "+91" + owner_phone_last10

    owner_name = str(row.get("ownerName", "")).strip()
    name_parts = owner_name.split()
    first_name = name_parts[0] if name_parts else ""
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    submitter_phone = str(row.get("submitterPhone", "")).strip()[-10:]
    if len(submitter_phone) == 10:
        submitter_phone = "+91" + submitter_phone

    upload_timestamp = int(time.time())
    upload_date = datetime.now().strftime("%Y-%m-%d")
    upload_time_readable = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    attributes = {
        "first_name": first_name,
        "last_name": last_name,
        "mobile": phone_with_code,
        "creation_date": str(row.get("creationDate", "")),
        "submitter_phone": submitter_phone,
        "incoming_state": str(row.get("incomingState", "")),
        "city": str(row.get("city", "")),
        "current_state": str(row.get("currentState", "")),
        "reject_reason": str(row.get("rejectReason", "")),
        "amount_paid": str(row.get("amountPaid", "")),
        "paid_on": str(row.get("paidOn", "")),
        "transaction_id": str(row.get("transactionId", "")),
        "property_id": str(row.get("listingId", "")),
        "sub_state": str(row.get("subState", "")),
        "group_type": "Ambassador Club NP cases",
        "upload_time": upload_timestamp,
        "upload_date": upload_date,
        "upload_time_readable": upload_time_readable
    }

    return {
        "type": "customer",
        "customer_id": phone_with_code,
        "attributes": attributes
    }, phone_with_code, owner_phone_last10

def push_to_moengage(row):
    payload, phone_with_code, phone_last10 = build_payload(row)

    if DRY_RUN:
        return row, phone_last10, True, None

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            return row, phone_last10, True, None
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else 0
            if status == 429:
                wait = RETRY_DELAY * (2 ** attempt)
                print(f"⚠️ Rate limited for {phone_with_code}, waiting {wait}s...")
                time.sleep(wait)
            elif attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY)
            else:
                return row, phone_last10, False, str(e)
        except Exception as e:
            if attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY)
            else:
                return row, phone_last10, False, str(e)

    return row, phone_last10, False, "Max retries exceeded"

def flush_to_sheet(batch_rows, dest_sheet, dest_cols, today_str, target_row):
    if DRY_RUN:
        print(f"🧪 [DRY RUN] Would write batch of {len(batch_rows)} rows starting at row {target_row}")
        return True

    rows_to_write = []
    for row in batch_rows:
        new_row = []
        for col in dest_cols:
            if col == "Remarks":
                new_row.append(today_str)
            else:
                new_row.append(row.get(col, ""))
        rows_to_write.append(new_row)

    range_name = f"A{target_row}"
    for attempt in range(RETRY_LIMIT):
        try:
            dest_sheet.update(range_name=range_name, values=rows_to_write, value_input_option="RAW")
            return True
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, 'response') else 0
            if status in (429, 500, 503) and attempt < RETRY_LIMIT - 1:
                wait = RETRY_DELAY * (2 ** attempt)
                print(f"⚠️ Sheet API error {status}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"❌ Failed to write batch to sheet: {e}")
                return False
    return False

success_batch = []
failed_rows = []
total_uploaded = 0
total_failed = 0

print(f"\n⚡ Starting concurrent upload with {MAX_WORKERS} workers...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(push_to_moengage, row): row for row in pending_rows}

    for future in as_completed(futures):
        row, phone_last10, success, error = future.result()
        phone_with_code = "+91" + phone_last10

        if success:
            uploaded_phones.add(phone_last10)
            success_batch.append(row)
            total_uploaded += 1
            print(f"✅ Uploaded: {phone_with_code} ({total_uploaded}/{len(pending_rows)})")

            if len(success_batch) >= BATCH_SIZE:
                print(f"📝 Writing batch of {BATCH_SIZE} rows to Google Sheet at row {next_row}...")
                if flush_to_sheet(success_batch, dest_sheet, dest_cols, today_str, next_row):
                    next_row += len(success_batch)
                success_batch = []
                time.sleep(1)
        else:
            total_failed += 1
            failed_rows.append(phone_with_code)
            print(f"❌ Failed: {phone_with_code} — {error}")

if success_batch:
    print(f"📝 Writing final batch of {len(success_batch)} rows to Google Sheet at row {next_row}...")
    if flush_to_sheet(success_batch, dest_sheet, dest_cols, today_str, next_row):
        next_row += len(success_batch)

print(f"\n{'='*50}")
print(f"✅ Successfully uploaded : {total_uploaded}")
print(f"❌ Failed                : {total_failed}")
if failed_rows:
    print(f"\n⚠️ Failed phones:")
    for p in failed_rows:
        print(f"   {p}")
print(f"{'='*50}")
