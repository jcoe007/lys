import requests
import csv
import os
import time
from datetime import datetime

API_URL = "https://api.em6.co.nz/ords/em6/data_api/free/price?"
CSV_FILE = "latest_data.csv"

def fetch_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"error {response.status_code}")
        return None

def get_latest_entry(data):
    if not data or "items" not in data:
        print("no valid data found")
        return None
    return max(data["items"], key=lambda x: x["trading_date"])

def save_to_csv(latest_entry, filename=CSV_FILE):
    if not latest_entry:
        print("no data to write")
        return

    trading_date = latest_entry["trading_date"]
    generation_data = latest_entry["generation_type"]
    fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # local time of fetch

    # flatten the generation_type into a single row
    row = {"fetch_time": fetch_time, "trading_date": trading_date}
    for gen in generation_data:
        for key, value in gen.items():
            row[key] = value

    # check if file exists to determine if headers need to be written
    file_exists = os.path.exists(filename)
    
    with open(filename, "a", newline="") as f:  # append mode
        writer = csv.DictWriter(f, fieldnames=row.keys())

        if not file_exists:
            writer.writeheader()  # write headers only once

        writer.writerow(row)  # write row

    print(f"appended latest data to {filename}")

# run
data = fetch_data()
latest_entry = get_latest_entry(data)

if latest_entry:
    save_to_csv(latest_entry)
