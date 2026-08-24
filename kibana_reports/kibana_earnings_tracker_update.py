import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# === CONFIGURATION ===
credentials_path = 'D:/Downloads/db-mismath-starship-data-ff9d8efaf2bb.json'
sheet_name = "Ambassador Earnings Tracker"

sheet_columns = {
    "Ramya New": "T",
    "Malik New": "V",
    "Dipali New": "T"
}

KIBANA_URL = "http://reports.nobroker.in:5609"
INDEX_NAME = "user"

output_csv = os.environ.get("OUTPUT_CSV_PATH", r"D:\NoBroker\Kibana\combined_user_states.csv")

# === GOOGLE SHEETS SETUP ===
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

# === HELPERS ===

def generate_formats(phone):
    phone = phone[-10:]
    return [phone, "91"+phone, "+91"+phone]


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
    headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}

    try:
        response = requests.post(url, json=query, headers=headers, timeout=60)
        data = response.json()

        result = {}
        for doc in data.get("hits", {}).get("hits", []):
            result[doc["_source"]["phone"][-10:]] = doc["_source"].get("userState", "Not Registered")

        return result

    except Exception as e:
        print("Fetch error:", e)
        return {}


# === MAIN PROCESS ===

def main():
    all_results = []

    for worksheet_name, output_col in sheet_columns.items():

        print(f"\nProcessing sheet: {worksheet_name}")

        sheet = client.open(sheet_name).worksheet(worksheet_name)
        records = sheet.get_all_records()

        df = pd.DataFrame(records)

        # column E
        if 'E' in df.columns:
            phone_numbers = df['E'].tolist()
        else:
            phone_numbers = df.iloc[:, 4].tolist()

        total_numbers = len(phone_numbers)

        chunk_size = 5000

        for start in range(0, total_numbers, chunk_size):

            end = min(start + chunk_size, total_numbers)

            batch_numbers = phone_numbers[start:end]

            formats = []
            valid_rows = {}

            for idx, phone in enumerate(batch_numbers, start=start+2):

                if phone is None or str(phone).strip() == "":
                    continue

                phone = str(phone).strip()

                if len(phone) < 10:
                    continue

                last10 = phone[-10:]

                valid_rows[idx] = last10

                formats.extend(generate_formats(last10))

            state_dict = fetch_user_state_batch(formats)

            batch_update = []

            for idx in range(start+2, end+2):

                if idx in valid_rows:

                    phone10 = valid_rows[idx]

                    state = state_dict.get(phone10, "Not Registered")

                    batch_update.append([state])

                    all_results.append({
                        "sheet": worksheet_name,
                        "phone": phone10,
                        "userState": state
                    })

                else:
                    batch_update.append([""])  # keep blank

            sheet_range = f"{output_col}{start+2}:{output_col}{end+1}"

            sheet.update(sheet_range, batch_update)

            print(f"{worksheet_name}: processed {end} rows")


    # === SAVE CSV ===

    df_csv = pd.DataFrame(all_results)

    if os.path.dirname(output_csv):
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df_csv.to_csv(output_csv, index=False)

    print("\nCSV saved to:", output_csv)

if __name__ == "__main__":
    main()
