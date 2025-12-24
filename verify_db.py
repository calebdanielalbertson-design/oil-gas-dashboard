import sqlite3
import pandas as pd

DB_NAME = "production.db"

def verify_database():
    print(f"Connecting to {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    
    print("\n--- Row Count Verification ---")
    count = pd.read_sql("SELECT count(*) as total_rows FROM production_data", conn)
    print(count.to_string(index=False))
    
    print("\n--- Date Range Verification ---")
    date_range = pd.read_sql("SELECT MIN(date) as first_date, MAX(date) as last_date FROM production_data", conn)
    print(date_range.to_string(index=False))

    print("\n--- Production by Year Analysis (Sample - Top 5 & Bottom 5 Years) ---")
    query = """
    SELECT 
        strftime('%Y', date) as year,
        SUM(bbls_oil) as total_oil_bbls
    FROM production_data 
    GROUP BY 1 
    ORDER BY 1
    """
    df = pd.read_sql(query, conn)
    print("First 5 Years:")
    print(df.head(5).to_string(index=False))
    print("\nLast 5 Years:")
    print(df.tail(5).to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    verify_database()
