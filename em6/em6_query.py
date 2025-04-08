import os
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

def fetch_energy_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def flatten_record(record):
    """
    Flattens a record by merging top-level keys with nested keys from 'generation_type'.
    """
    flattened = {
        "trading_date": record.get("trading_date"),
        "grid_zone_id": record.get("grid_zone_id"),
        "grid_zone_name": record.get("grid_zone_name")
    }
    for gen in record.get("generation_type", []):
        for key, value in gen.items():
            flattened[key] = value
    return flattened

def append_latest_record(csv_file, record):
    """
    Appends the new record to the CSV file.
    If the file does not exist, it creates it (with headers).
    """
    df_new = pd.DataFrame([record])
    if not os.path.isfile(csv_file):
        df_new.to_csv(csv_file, index=False, mode='w')
    else:
        df_new.to_csv(csv_file, index=False, mode='a', header=False)

def compute_mwh_differences(csv_file):
    """
    Reads the CSV file, removes any previously computed delta columns (ending in _delta),
    computes the delta (difference) for each _mwh column (rounded to 2 decimal places),
    and overwrites the CSV.
    """
    df = pd.read_csv(csv_file, parse_dates=["trading_date", "run_time"])
    delta_cols = [col for col in df.columns if col.endswith('_delta')]
    if delta_cols:
        df = df.drop(columns=delta_cols)
    
    mwh_cols = [col for col in df.columns if col.endswith('_mwh')]
    for col in mwh_cols:
        df[col + "_delta"] = df[col].diff().round(2)
    
    df.to_csv(csv_file, index=False)
    return df

def main():
    url = "https://api.em6.co.nz/ords/em6/data_api/free/price?"
    csv_file = "nz_energy_data.csv"
    
    # Fetch and flatten the latest record.
    data = fetch_energy_data(url)
    items = data.get("items", [])
    if not items:
        print("No items found in the data.")
        return

    most_recent = max(items, key=lambda r: pd.to_datetime(r.get("trading_date")))
    record = flatten_record(most_recent)
    record["run_time"] = datetime.now(ZoneInfo("Pacific/Auckland")).isoformat()
    
    # Before appending, check if the _mwh fields in the new record are identical
    # (after rounding to 2 decimal places) to those in the last row.
    if os.path.isfile(csv_file):
        try:
            df_existing = pd.read_csv(csv_file)
            if not df_existing.empty:
                # Get list of _mwh fields from the new record.
                mwh_fields = [col for col in record.keys() if col.endswith('_mwh')]
                last_row = df_existing.iloc[-1]
                same = True
                for field in mwh_fields:
                    try:
                        val_csv = round(float(last_row[field]), 2)
                        val_record = round(float(record[field]), 2)
                        if val_csv != val_record:
                            same = False
                            break
                    except (ValueError, TypeError, KeyError) as e:
                        # If conversion fails or key is missing, assume they are different.
                        same = False
                        break
                        
                if same:
                    print("Data source has not updated the _mwh values since the last run. Skipping append.")
                    return
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
    
    # Append the new record to the CSV.
    append_latest_record(csv_file, record)
    print(f"Record appended to {csv_file}.")
    
    # Re-read the CSV, compute the delta values (rounded to 2 d.p.), and overwrite the CSV.
    df_updated = compute_mwh_differences(csv_file)
    print("Delta columns computed (rounded to 2 decimal places) and updated.")


if __name__ == "__main__":
    main()
