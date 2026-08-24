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

# Pause after every 1000 uploads
UPLOAD_CHUNK_SIZE = 1000

# When true, never prompt for input — just run straight through all chunks.
NON_INTERACTIVE = os.environ.get("NON_INTERACTIVE", "true").lower() == "true"

# When true, do everything except actually POST to MoEngage / write
# uploads to the sheet.
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

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

# ==============================
# SPREADSHEETS
# ==============================
try:
    ss1 = client.open("Acquisition Campaigns and Calling Ambassador-2")
except Exception as e:
    print(f"❌ Failed to open spreadsheet 'Acquisition Campaigns and Calling Ambassador-2': {e}")
    sys.exit(1)

try:
    ss2 = client.open("Acquisition Campaigns and Calling Ambassador")
except Exception as e:
    print(f"❌ Failed to open spreadsheet 'Acquisition Campaigns and Calling Ambassador': {e}")
    sys.exit(1)

# ==============================
# DESTINATION SHEETS SETUP
# ==============================
def get_or_create_dest_sheet(spreadsheet, dest_tab_name):
    try:
        dest_sheet = spreadsheet.worksheet(dest_tab_name)
    except gspread.WorksheetNotFound:
        dest_sheet = spreadsheet.add_worksheet(
            title=dest_tab_name, rows="100000", cols="20"
        )
    if not dest_sheet.get_all_values():
        dest_sheet.append_row([
            "City", "Society name", "Name", "E-mail",
            "Phone Number", "Month", "Date", "Remarks"
        ])
    return dest_sheet

dest_sheet1 = get_or_create_dest_sheet(ss1, "Moengage")
dest_sheet2 = get_or_create_dest_sheet(ss2, "Moengage")

# ==============================
# BUILD SET OF UPLOADED PHONES
# ==============================
uploaded_phones = set()

def load_uploaded_phones(dest_sheet, sheet_label):
    all_values = dest_sheet.get_all_values()
    if len(all_values) > 1:
        headers_row = [str(h).strip() for h in all_values[0]]
        try:
            phone_col_index = headers_row.index("Phone Number")
            for row in all_values[1:]:
                if len(row) > phone_col_index:
                    raw_phone = str(row[phone_col_index]).strip()
                    digits = ''.join(filter(str.isdigit, raw_phone))
                    if len(digits) >= 10:
                        uploaded_phones.add(digits[-10:])
        except ValueError:
            print(f"❌ 'Phone Number' column not found in Moengage tab of {sheet_label}")

load_uploaded_phones(dest_sheet1, "Ambassador-2")
load_uploaded_phones(dest_sheet2, "Ambassador")

print(f"📋 Total unique uploaded numbers found across both Moengage tabs: {len(uploaded_phones)}")

# ==============================
# THREAD LOCK & HELPERS
# ==============================
sheet_lock = Lock()

def get_row_value(row, possible_keys):
    for key in possible_keys:
        if key in row:
            return row[key]
        # Try stripped/case-insensitive
        key_stripped = key.strip().lower()
        for rk in row.keys():
            if str(rk).strip().lower() == key_stripped:
                return row[rk]
    return ""

def clean_phone(phone):
    digits = ''.join(filter(str.isdigit, str(phone)))
    if len(digits) >= 10:
        return digits[-10:]
    return None

# ==============================
# DYNAMIC MONTH GENERATION & CYCLE CALCULATION
# ==============================
def get_ist_now():
    # UTC + 5.5 hours
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def generate_historical_months():
    months = []
    
    # 1. Society Manager_Acquisition (12-2022 to 11-2023)
    months.append(("Acquisition Campaigns and Calling Ambassador", "Society Manager_Acquisition", "12-2022"))
    for m in range(1, 12):
        months.append(("Acquisition Campaigns and Calling Ambassador", "Society Manager_Acquisition", f"{m}-2023"))
        
    # 2. FB Campaign_Acquisition_Nov23 (11-2023 to 12-2024)
    months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Nov23", "11-2023"))
    months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Nov23", "12-2023"))
    for m in range(1, 13):
        months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Nov23", f"{m}-2024"))
        
    # 3. FB Campaign_Acquisition_Jan25 (1-2025 to 10-2025)
    for m in range(1, 11):
        months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Jan25", f"{m}-2025"))
        
    # 4. FB Campaign_Acquisition_Nov25 (11-2025 to 3-2026)
    months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Nov25", "11-2025"))
    months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Nov25", "12-2025"))
    for m in range(1, 4):
        months.append(("Acquisition Campaigns and Calling Ambassador", "FB Campaign_Acquisition_Nov25", f"{m}-2026"))
        
    # 5. FB Campaign_Acquisition_Apr26 (3-2026 to previous month)
    now_ist = get_ist_now()
    current_year = now_ist.year
    current_month = now_ist.month
    
    start_year = 2026
    start_month = 3
    
    while True:
        if start_year == current_year and start_month == current_month:
            break
        months.append(("Acquisition Campaigns and Calling Ambassador-2", "FB Campaign_Acquisition_Apr26", f"{start_month}-{start_year}"))
        
        start_month += 1
        if start_month > 12:
            start_month = 1
            start_year += 1
            
    return months

# Calculate cycle day
reference_date = datetime(2026, 1, 1)
now_ist = get_ist_now()
days_since_epoch = (now_ist - reference_date).days

cycle_day = days_since_epoch % 11
is_current_month_push_day = (days_since_epoch % 3 == 0)

# Allow manual overrides
cycle_day_env = os.environ.get("CYCLE_DAY")
if cycle_day_env is not None and cycle_day_env.strip() != "":
    cycle_day = int(cycle_day_env)
    print(f"🔧 Overridden CYCLE_DAY = {cycle_day}")

push_current_month_env = os.environ.get("PUSH_CURRENT_MONTH")
if push_current_month_env is not None and push_current_month_env.strip() != "":
    is_current_month_push_day = push_current_month_env.lower() in ["true", "1", "yes"]
    print(f"🔧 Overridden PUSH_CURRENT_MONTH = {is_current_month_push_day}")

historical_months = generate_historical_months()
num_months = len(historical_months)

# Partition 46+ historical months into 11 chunks (cycle day 0 to 10)
chunk_size = 4
day_targets = []
for i in range(11):
    start = i * chunk_size
    if i == 10:
        end = num_months
    else:
        end = start + chunk_size
    day_targets.append(historical_months[start:end])

# Target worksheets and months for today
today_targets = list(day_targets[cycle_day])

current_month_str = f"{now_ist.month}-{now_ist.year}"
current_month_target = ("Acquisition Campaigns and Calling Ambassador-2", "FB Campaign_Acquisition_Apr26", current_month_str)

if is_current_month_push_day:
    print(f"📅 Today is a current month push day! Adding {current_month_str} target.")
    today_targets.append(current_month_target)
else:
    print(f"📅 Today is NOT a current month push day (runs every 3 days). Only running cycle day {cycle_day} targets.")

print("\n🎯 Targets for today:")
for t in today_targets:
    print(f"  - Spreadsheet: {t[0]} | Worksheet: {t[1]} | Month: {t[2]}")

# ==============================
# ELIGIBILITY & PAYLOAD LOGIC
# ==============================
def get_row_status(row, target_month):
    phone = clean_phone(get_row_value(row, ['Phone Number Corrected', 'Phone Number_Correct', 'Phone Number']))
    kibana = str(get_row_value(row, ['Kibana Status'])).lower()
    ab_id = str(get_row_value(row, ['AB ID'])).strip()
    month = str(get_row_value(row, ['Month'])).strip()

    if not phone:
        return "invalid_phone"

    if "broker" in kibana or "builder" in kibana:
        return "filtered_out"

    if ab_id != "-":
        return "filtered_out"

    if month != target_month:
        return "filtered_out"

    if phone in uploaded_phones:
        return "already_uploaded"

    return "eligible"

def build_payload(row):
    phone_last10 = clean_phone(get_row_value(row, ['Phone Number Corrected', 'Phone Number_Correct', 'Phone Number']))
    phone_with_code = "+91" + phone_last10

    name = str(get_row_value(row, [' Name', 'Name', 'Lead Name'])).strip()
    parts = name.split()
    first_name = parts[0] if parts else ""
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""

    now = datetime.now()

    attributes = {
        "first_name": first_name,
        "last_name": last_name,
        "mobile": phone_with_code,
        "email": str(get_row_value(row, ['E-mail ', 'E-mail', 'Lead Email'])).strip(),
        "city": str(get_row_value(row, ['City'])).strip(),
        "society_name": str(get_row_value(row, ['Society name'])).strip(),
        "month": str(get_row_value(row, ['Month'])).strip(),
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

def flush_to_sheet(rows_with_remarks, spreadsheet_name):
    if not rows_with_remarks:
        return
    if DRY_RUN:
        print(f"🧪 [DRY RUN] Would log {len(rows_with_remarks)} rows to {spreadsheet_name} Moengage tab")
        return

    today_str = datetime.now().strftime("%d-%b-%Y")
    data = []
    for row, remarks in rows_with_remarks:
        phone_raw = get_row_value(row, ['Phone Number Corrected', 'Phone Number_Correct', 'Phone Number'])
        city = get_row_value(row, ['City'])
        society_name = get_row_value(row, ['Society name'])
        name = get_row_value(row, [' Name', 'Name', 'Lead Name'])
        email = get_row_value(row, ['E-mail ', 'E-mail', 'Lead Email'])
        month = get_row_value(row, ['Month'])
        
        data.append([
            city,
            society_name,
            str(name).strip(),
            str(email).strip(),
            str(phone_raw),
            month,
            today_str,
            remarks
        ])

    dest_sheet = dest_sheet1 if spreadsheet_name == "Acquisition Campaigns and Calling Ambassador-2" else dest_sheet2

    for attempt in range(RETRY_LIMIT):
        try:
            with sheet_lock:
                dest_sheet.append_rows(data, value_input_option="RAW")
            return
        except Exception as e:
            if attempt < RETRY_LIMIT - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ Sheet write failed for {spreadsheet_name}: {e}")

# ==============================
# MAIN PROCESSING LOOP
# ==============================
overall_eligible = 0
overall_uploaded = 0
overall_failed = 0
overall_duplicates = 0

for target in today_targets:
    ss_name, ws_name, target_month = target
    
    print("\n" + "=" * 60)
    print(f"📖 Processing: {ss_name} | {ws_name} | Month: {target_month}")
    print("=" * 60)
    
    try:
        ss = ss1 if ss_name == "Acquisition Campaigns and Calling Ambassador-2" else ss2
        ws = ss.worksheet(ws_name)
        records = ws.get_all_records()
    except Exception as e:
        print(f"❌ Failed to load Worksheet '{ws_name}' from Spreadsheet '{ss_name}': {e}")
        continue
        
    print(f"📊 Loaded {len(records)} total records from worksheet")
    
    eligible_rows = []
    already_uploaded = []
    
    for row in records:
        status = get_row_status(row, target_month)
        if status == "eligible":
            eligible_rows.append(row)
        elif status == "already_uploaded":
            already_uploaded.append(row)
            
    print(f"🟢 Eligible to upload      : {len(eligible_rows)}")
    print(f"🔵 Already in Moengage tab : {len(already_uploaded)}")
    
    overall_eligible += len(eligible_rows)
    overall_duplicates += len(already_uploaded)
    
    # Log skipped rows
    if already_uploaded:
        flush_to_sheet(
            [(row, "Already in Moengage tab") for row in already_uploaded],
            ss_name
        )
        print(f"📝 Logged {len(already_uploaded)} duplicate rows to sheet")
        
    if not eligible_rows:
        print("⏭️ No eligible rows to upload for this target.")
        continue
        
    print(f"🚀 Starting upload for {len(eligible_rows)} eligible rows...")
    
    success_batch = []
    target_uploaded = 0
    target_failed = 0
    
    for chunk_start in range(0, len(eligible_rows), UPLOAD_CHUNK_SIZE):
        chunk_end = chunk_start + UPLOAD_CHUNK_SIZE
        current_chunk = eligible_rows[chunk_start:chunk_end]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(push_to_moengage, row) for row in current_chunk]
            
            for future in as_completed(futures):
                row, phone, success, error = future.result()
                
                if success:
                    uploaded_phones.add(phone)
                    success_batch.append((row, "Uploaded"))
                    target_uploaded += 1
                    overall_uploaded += 1
                    print(f"✅ Uploaded: +91{phone} (Current sheet: {target_uploaded} | Overall: {overall_uploaded})")
                else:
                    success_batch.append((row, f"Upload Failed: {error}"))
                    target_failed += 1
                    overall_failed += 1
                    print(f"❌ Failed: +91{phone} — {error}")
                    
                if len(success_batch) >= BATCH_SIZE:
                    flush_to_sheet(success_batch, ss_name)
                    success_batch = []
                    time.sleep(1)
                    
        # Final chunk flush
        if success_batch:
            flush_to_sheet(success_batch, ss_name)
            success_batch = []

# ==============================
# GLOBAL RUN SUMMARY
# ==============================
print("\n" + "=" * 50)
print("🏁 GLOBAL RUN SUMMARY")
print(f"🟢 Total eligible found    : {overall_eligible}")
print(f"✅ Total uploaded          : {overall_uploaded}")
print(f"❌ Total upload failed     : {overall_failed}")
print(f"🔵 Total already uploaded  : {overall_duplicates}")
print("=" * 50)
