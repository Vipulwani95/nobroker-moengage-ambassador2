import os
import sys
import re
import requests
from datetime import datetime, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# === CONFIG ===
KIBANA_URL = "http://reports.nobroker.in:5609"
INDEX_NAME = "crowd_source_lead"
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

# Google Sheets
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
credentials_path = 'D:/Downloads/db-mismath-starship-data-ff9d8efaf2bb.json'

def get_ist_now():
    # Convert current time to IST to avoid UTC boundary issues
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_previous_month_range_and_name():
    ist_now = get_ist_now()
    first_day_current = ist_now.replace(day=1)
    last_day_prev = first_day_current - timedelta(days=1)
    
    start_date = last_day_prev.replace(day=1)
    end_date = last_day_prev
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    sheet_name = start_date.strftime("%b-%Y")
    
    return start_date_str, end_date_str, sheet_name

start_date_str, end_date_str, sheet_name_1 = get_previous_month_range_and_name()

print(f"📅 Target Previous Month Date Range: {start_date_str} to {end_date_str}")
print(f"📊 Target Sheet Name: {sheet_name_1}")

# Other sheets to write same data (disabled for this run)
other_sheets = []

# RM lookup source (same as the Jul-2026 script)
LOGIN_DETAILS_SHEET_ID = "1G5LG52oOrpwHNIUc1MhnmAYUsd2h_2I8woIToZ-hMHE"
LOGIN_DETAILS_TAB = "Login Details"
LOGIN_PHONE_COL = 0   # column A -> phone
LOGIN_RM_COL = 7      # column H -> RM name (matches VLOOKUP col_index 8 in your formula)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

if GOOGLE_SERVICE_ACCOUNT_JSON:
    key_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
else:
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)

client = gspread.authorize(creds)

try:
    sheet_1 = client.open(sheet_name_1).worksheet(sheet_name_1)
except (gspread.SpreadsheetNotFound, gspread.WorksheetNotFound) as e:
    print(f"⚠️ Google Sheet or Worksheet '{sheet_name_1}' was not found. Gracefully halting execution as requested. Details: {e}")
    sys.exit(0)

# Columns to write (A:N) - N is now the RM name, computed in Python
FIELDS = [
    "creationDate", "submitterPhone", "ownerPhone", "ownerName",
    "incomingState", "city", "currentState", "rejectReason",
    "amountPaid", "paidOn", "transactionId", "listingId", "subState",
    "RM"
]

BATCH_SIZE = 8000
SCROLL_TIMEOUT = "2m"

# Test Limit configuration
TEST_LIMIT = os.environ.get("TEST_LIMIT")
if TEST_LIMIT:
    try:
        TEST_LIMIT = int(TEST_LIMIT)
        print(f"⚠️ Running in TEST mode. Row limit set to: {TEST_LIMIT}")
    except ValueError:
        TEST_LIMIT = None

if TEST_LIMIT:
    BATCH_SIZE = TEST_LIMIT

# --- HELPERS ---
def format_phone(number):
    if not number:
        return ""
    number = str(number).replace(" ", "").replace("-", "")
    if number.startswith("0"):
        number = number[1:]
    if not number.startswith("+91"):
        number = "+91" + number
    return number

def normalize_last10(number):
    """Strip non-digits and keep the last 10 digits, matching the sheet's
    RIGHT(REGEXREPLACE(..., "[^0-9]", ""), 10) logic."""
    if not number:
        return ""
    digits = re.sub(r"[^0-9]", "", str(number))
    return digits[-10:] if digits else ""

def format_date_mmddyyyy(ms):
    if not ms:
        return ""
    return datetime.fromtimestamp(ms / 1000).strftime("%m/%d/%Y")

def build_rm_lookup():
    """Reads the Login Details tab once and builds {last10digits: RM name}."""
    print("Loading RM lookup from Login Details tab...")
    try:
        login_ws = client.open_by_key(LOGIN_DETAILS_SHEET_ID).worksheet(LOGIN_DETAILS_TAB)
        all_rows = login_ws.get_all_values()
    except Exception as e:
        print(f"Failed to load RM lookup: {e}")
        sys.exit(1)

    rm_map = {}
    for row in all_rows[1:]:  # skip header row
        if len(row) <= max(LOGIN_PHONE_COL, LOGIN_RM_COL):
            continue
        phone_key = normalize_last10(row[LOGIN_PHONE_COL])
        rm_name = row[LOGIN_RM_COL].strip()
        if phone_key and rm_name:
            rm_map[phone_key] = rm_name

    print(f"RM lookup loaded: {len(rm_map)} phone numbers mapped")
    return rm_map

def write_batch(ws, batch_rows, start_row, end_row):
    """Writes the full row as RAW (so B/C phone numbers and everything else
    stay exactly as before), then fixes ONLY columns A and J (the dates) with
    USER_ENTERED so they become real dates without touching anything else."""
    ws.update(values=batch_rows, range_name=f"A{start_row}:N{end_row}", value_input_option="RAW")

    date_col_a = [[row[0]] for row in batch_rows]   # creationDate
    date_col_j = [[row[9]] for row in batch_rows]   # paidOn

    ws.batch_update(
        [
            {"range": f"A{start_row}:A{end_row}", "values": date_col_a},
            {"range": f"J{start_row}:J{end_row}", "values": date_col_j},
        ],
        value_input_option="USER_ENTERED"
    )

def transform_hit(hit, rm_map):
    src = hit.get("_source", {})
    submitter_phone = src.get("phone")
    rm_name = rm_map.get(normalize_last10(submitter_phone), "")

    return [
        format_date_mmddyyyy(src.get("creationDate")),
        format_phone(submitter_phone),        # submitterPhone
        format_phone(src.get("ownerPhone")),
        src.get("ownerName") or "",
        src.get("incomingState") or "",
        src.get("city") or "",
        src.get("currentState") or "",
        src.get("rejectReason") or "",
        src.get("amountPaid") or 0,
        format_date_mmddyyyy(src.get("paidOn")),        # paidOn in MM/DD/YYYY
        src.get("transactionId") or "",
        src.get("listingId") or "",
        src.get("subState") or "",
        rm_name
    ]

# --- SCROLL INITIALIZATION ---
def start_scroll(start_date_str, end_date_str):
    """
    start_date_str, end_date_str: 'YYYY-MM-DD'
    """
    month_start = f"{start_date_str}T00:00:00"
    month_end = f"{end_date_str}T23:59:59"

    query = {
        "size": BATCH_SIZE,
        "_source": True,
        "query": {
            "bool": {
                "filter": [
                    {"query_string": {"query": "type.raw:LISTING_AMB", "analyze_wildcard": True}},
                    {"range": {"creationDate": {"gte": month_start, "lte": month_end}}}
                ]
            }
        },
        "sort": [{"creationDate": "asc"}]
    }

    url = f"{KIBANA_URL}/api/console/proxy?path={INDEX_NAME}/_search?scroll={SCROLL_TIMEOUT}&method=POST"
    response = requests.post(url, headers=HEADERS, json=query, timeout=180)
    data = response.json()
    return data["_scroll_id"], data["hits"]["hits"]

def continue_scroll(scroll_id):
    url = f"{KIBANA_URL}/api/console/proxy?path=_search/scroll&method=POST"
    payload = {"scroll": SCROLL_TIMEOUT, "scroll_id": scroll_id}
    response = requests.post(url, headers=HEADERS, json=payload, timeout=180)
    data = response.json()
    return data.get("_scroll_id"), data.get("hits", {}).get("hits", [])

# --- MAIN ---
def main(start_date_str, end_date_str):
    rm_map = build_rm_lookup()

    scroll_id, hits = start_scroll(start_date_str, end_date_str)
    batch_num = 1
    total_fetched = 0

    # Clear first sheet and write headers (A:N now, includes RM)
    sheet_1.clear()
    sheet_1.update(values=[FIELDS], range_name="A1:N1")

    # Prepare other sheets objects (empty if other_sheets is disabled above)
    other_ws = []
    for sh in other_sheets:
        wb = client.open(sh["file_name"])
        ws = wb.worksheet(sh["tab_name"])
        other_ws.append(ws)

    # Memory to store all batches
    all_batches = []

    while hits:
        # Transform batch
        batch_rows = [transform_hit(hit, rm_map) for hit in hits]
        
        if TEST_LIMIT and len(batch_rows) > TEST_LIMIT:
            batch_rows = batch_rows[:TEST_LIMIT]
            
        all_batches.append(batch_rows)  # store in memory

        start_row = total_fetched + 2
        end_row = start_row + len(batch_rows) - 1

        # Write to first sheet immediately (RAW for everything, USER_ENTERED only for date cols A & J)
        write_batch(sheet_1, batch_rows, start_row, end_row)

        total_fetched += len(batch_rows)
        print(f"Fetched batch {batch_num} ({len(batch_rows)} records, total so far: {total_fetched})")
        batch_num += 1

        if TEST_LIMIT and total_fetched >= TEST_LIMIT:
            print(f"Reached test limit ({TEST_LIMIT} rows). Exiting query loop.")
            break

        time.sleep(3)  # avoid throttling

        # Continue scroll
        scroll_id, hits = continue_scroll(scroll_id)

    # Now write stored batches to other sheets (skipped if other_sheets is empty)
    for ws in other_ws:
        print(f"\nWriting all stored batches to sheet '{ws.title}' ...")
        total_rows_written = 0
        for batch_rows in all_batches:
            start_row = total_rows_written + 2
            end_row = start_row + len(batch_rows) - 1
            write_batch(ws, batch_rows, start_row, end_row)
            total_rows_written += len(batch_rows)
            time.sleep(5)  # 5s delay between batch writes
        print(f"✅ Finished writing {total_rows_written} rows to sheet '{ws.title}'")

    print(f"\n✅ All sheets updated successfully with {total_fetched} rows")

# === RUN SCRIPT ===
if __name__ == "__main__":
    main(start_date_str, end_date_str)
