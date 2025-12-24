import pandas as pd
import sqlite3
import os

DB_NAME = "production.db"
CSV_FILE = r"C:\Users\User\Desktop\Production data 1-2000_-_12-2023v2 Update Status.csv"

# Column Mapping: CSV Column -> DB Column
COLUMN_MAPPING = {
    'File No': 'file_no',
    'API_WELLNO': 'api_no',
    'Pool Name': 'pool',
    'RPT_DATE': 'date',
    'BBLS_OIL_COND': 'bbls_oil',
    'BBLS_WTR': 'bbls_water',
    'MCF_GAS': 'mcf_gas',
    'DAYS_PROD': 'days_produced',
    'OIL_RUNS': 'oil_sold',
    'MCF_SOLD': 'mcf_sold',
    'FLARED': 'mcf_flared'
}

def import_historical_data():
    print("--- Starting Historical Data Import ---")
    
    if not os.path.exists(CSV_FILE):
        print(f"Error: File not found at {CSV_FILE}")
        return

    print(f"Reading CSV: {os.path.basename(CSV_FILE)}...")
    # Low_memory=False to avoid mixed type warnings on large files if any
    try:
        df = pd.read_csv(CSV_FILE, usecols=COLUMN_MAPPING.keys(), low_memory=False)
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        return

    print(f"  - Rows read: {len(df)}")
    
    # Rename columns
    print("Renaming columns...")
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    
    # Convert types
    print("Converting data types...")
    # Date conversion
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Fill NaNs for numeric columns if appropriate, or leave as None (SQLite handles NULL)
    # For now, we will perform a basic clean for safety:
    # Ensure numeric columns are numeric, coerce errors to NaN
    numeric_cols = ['file_no', 'api_no', 'bbls_oil', 'bbls_water', 'mcf_gas', 
                    'days_produced', 'oil_sold', 'mcf_sold', 'mcf_flared']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print("Appending to database...")
    conn = sqlite3.connect(DB_NAME)
    
    # Append to existing table
    try:
        df.to_sql('production_data', conn, if_exists='append', index=False)
        print("Success! Data appended.")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

    print("--- Import Complete ---")

if __name__ == "__main__":
    import_historical_data()
