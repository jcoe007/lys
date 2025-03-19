import requests
import psycopg2
import os
import json
from datetime import datetime

# API URL
API_URL = "https://api.em6.co.nz/ords/em6/data_api/free/price?"

# Get database credentials from environment variables
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_port = os.environ.get('DB_PORT', '5432')

def fetch_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def get_latest_entry(data):
    if not data or "items" not in data:
        print("No valid data found")
        return None
    return max(data["items"], key=lambda x: x["trading_date"])

def save_to_postgres(latest_entry):
    if not latest_entry:
        print("No data to write")
        return
    
    trading_date = latest_entry["trading_date"]
    generation_data = latest_entry["generation_type"]
    fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # local time of fetch
    
    # Flatten the generation_type into a single row
    row = {"fetch_time": fetch_time, "trading_date": trading_date}
    for gen in generation_data:
        for key, value in gen.items():
            row[key] = value
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        
        with conn.cursor() as cur:
            # Check if table exists, if not create it
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'energy_data'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table with dynamic columns based on our data
                columns = []
                for key, value in row.items():
                    # Determine column type based on value
                    if isinstance(value, int):
                        columns.append(f"{key} INTEGER")
                    elif isinstance(value, float):
                        columns.append(f"{key} NUMERIC")
                    else:
                        columns.append(f"{key} TEXT")
                
                create_table_sql = f"""
                    CREATE TABLE energy_data (
                        id SERIAL PRIMARY KEY,
                        {', '.join(columns)}
                    );
                """
                cur.execute(create_table_sql)
                print("Created energy_data table")
            
            # Prepare INSERT statement
            columns = list(row.keys())
            placeholders = [f"%({key})s" for key in columns]
            
            insert_sql = f"""
                INSERT INTO energy_data ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            # Execute INSERT
            cur.execute(insert_sql, row)
            conn.commit()
            print(f"Inserted latest data with trading_date: {trading_date}")
            
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# Run the script
if __name__ == "__main__":
    print(f"Script started at {datetime.now()}")
    data = fetch_data()
    latest_entry = get_latest_entry(data)
    if latest_entry:
        save_to_postgres(latest_entry)
    print(f"Script completed at {datetime.now()}")
