import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import csv
import time
import json
from gspread.exceptions import APIError

# ================= CONFIG =================

credentials_path = 'D:/Downloads/db-mismath-starship-data-ff9d8efaf2bb.json'
sheet_name = "Acquisition Campaigns and Calling Ambassador-2"
sheet_columns = {
    "FB Campaign_Acquisition_Apr26": "P"
}
KIBANA_URL = "http://reports.nobroker.in:5609"
INDEX_NAME = "user"
output_csv = os.environ.get("OUTPUT_CSV_PATH", r"D:\NoBroker\Kibana\combined_user_states.csv")

# ================= GOOGLE AUTH =================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
if GOOGLE_SERVICE_ACCOUNT_JSON:
    key_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
else:
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)

client = gspread.authorize(creds)

# ================= RETRY HELPER =================

def with_retry(fn, retries=5, backoff=5):
    """Calls fn(), retrying on 503 APIError with exponential backoff."""
    for attempt in range(retries):
        try:
            return fn()
        except APIError as e:
            if "503" in str(e) or "502" in str(e):
                wait = backoff * (2 ** attempt)
                print(f"  [Retry {attempt+1}/{retries}] Google API unavailable, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise  # Don't retry other API errors (403, 404, etc.)
    raise RuntimeError(f"Failed after {retries} retries")

# ================= HELPERS =================

def generate_formats(phone):
    phone = phone[-10:]
    return [phone, "91" + phone, "+91" + phone]


def fetch_user_state_batch(phones):
    query = {
        "_source": ["phone", "userState"],
        "query": {
            "bool": {
                "filter": {
                    "terms": {
                        "phone.raw": phones
                    }
                }
            }
        },
        "size": len(phones)
    }
    url = f"{KIBANA_URL}/api/console/proxy?path={INDEX_NAME}/_search&method=POST"
    headers = {
        "kbn-xsrf": "true",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, json=query, headers=headers, timeout=60)
        data = response.json()
        result = {}
        for doc in data.get("hits", {}).get("hits", []):
            phone10 = doc["_source"]["phone"][-10:]
            result[phone10] = doc["_source"].get("userState", "Not Registered")
        return result
    except Exception as e:
        print("Fetch error:", e)
        return {}

# ================= MAIN =================

def main():
    all_results = []

    for worksheet_name, output_col in sheet_columns.items():
        print(f"\nProcessing: {worksheet_name}")

        # ---- Retry opening worksheet ----
        sheet = with_retry(lambda: client.open(sheet_name).worksheet(worksheet_name))

        # ---- Retry reading column J ----
        phone_numbers = with_retry(lambda: sheet.col_values(10)[1:])

        total_numbers = len(phone_numbers)
        print("Total rows:", total_numbers)

        chunk_size = 3000
        futures = []
        updates = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            for start in range(0, total_numbers, chunk_size):
                end = min(start + chunk_size, total_numbers)
                batch_numbers = phone_numbers[start:end]
                formats = []
                valid_rows = {}

                for idx, phone in enumerate(batch_numbers, start=start + 2):
                    if not phone:
                        continue
                    phone = str(phone).strip()
                    if len(phone) < 10:
                        continue
                    last10 = phone[-10:]
                    valid_rows[idx] = last10
                    formats.extend(generate_formats(last10))

                future = executor.submit(fetch_user_state_batch, formats)
                futures.append((future, valid_rows))

            for future, valid_rows in futures:
                state_dict = future.result()
                for row_num, phone10 in valid_rows.items():
                    state = state_dict.get(phone10, "Not Registered")
                    updates[row_num] = state
                    all_results.append({
                        "sheet": worksheet_name,
                        "phone": phone10,
                        "userState": state
                    })

        # ================= BULK UPDATE with retry =================

        batch_data = [
            {"range": f"{output_col}{row_num}", "values": [[state]]}
            for row_num, state in updates.items()
        ]

        if batch_data:
            with_retry(lambda: sheet.batch_update(batch_data))

        print(f"Completed: {worksheet_name}")

    # ================= SAVE CSV =================

    if os.path.dirname(output_csv):
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["sheet", "phone", "userState"])
        writer.writeheader()
        writer.writerows(all_results)

    print("\nCSV saved:", output_csv)

if __name__ == "__main__":
    main()
