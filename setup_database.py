import pandas as pd
import sqlite3
import os

DB_NAME = "production.db"
FILE_2024 = r"C:\Users\User\Documents\DMR production 2024.xlsx"
FILE_2025 = r"C:\Users\User\Documents\DMR production 2025 oct.xlsx"

def clean_column_name(col):
    return col.strip().lower().replace(' ', '_')

def setup_database():
    print("--- Starting Database Setup ---")
    
    # 1. Load 2024 data (Truth for Columns)
    print(f"Reading {os.path.basename(FILE_2024)}...")
    df_2024 = pd.read_excel(FILE_2024)
    print(f"  - Rows: {len(df_2024)}")
    
    correct_columns = df_2024.columns.tolist()
    print(f"  - Detected Columns: {correct_columns}")

    # 2. Load 2025 data (Missing Headers)
    print(f"Reading {os.path.basename(FILE_2025)}...")
    # header=None means read first row as data.
    # We assign names parameter to force our column names.
    df_2025 = pd.read_excel(FILE_2025, header=None, names=correct_columns)
    print(f"  - Rows: {len(df_2025)}")

    # 3. Combine DataFrames
    print("Combining datasets...")
    df_combined = pd.concat([df_2024, df_2025], ignore_index=True)
    
    # 4. Clean Data
    print("Cleaning data...")
    # Clean column names
    df_combined.columns = [clean_column_name(c) for c in df_combined.columns]
    
    # Ensure date is datetime
    if 'date' in df_combined.columns:
        df_combined['date'] = pd.to_datetime(df_combined['date'])
        
    print(f"Total Rows to Import: {len(df_combined)}")
    print(f"Final Columns: {df_combined.columns.tolist()}")

    # 5. Write to SQLite
    print(f"Writing to SQLite database: {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    
    # if_exists='replace' will drop the table if it exists and create new
    df_combined.to_sql('production_data', conn, if_exists='replace', index=False)
    
    # Create Indices for performance
    print("Creating indices...")
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_no ON production_data (api_no)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON production_data (date)")
    conn.commit()
    conn.close()
    
    print("--- Database Setup Complete ---")

if __name__ == "__main__":
    setup_database()
