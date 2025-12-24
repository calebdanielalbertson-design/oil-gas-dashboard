import sqlite3
import pandas as pd

DB_NAME = "production.db"

def verify_status():
    print(f"Connecting to {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    
    print("\n--- Status Distribution ---")
    dist = pd.read_sql("SELECT status, count(*) as count FROM production_data GROUP BY 1 ORDER BY 2 DESC", conn)
    print(dist.to_string(index=False))
    
    print("\n--- Sample Validation: Inactive Well (AB) ---")
    # Find a well that has 'AB' status recently
    query_ab = """
    SELECT * FROM production_data 
    WHERE status = 'AB' 
    LIMIT 1
    """
    ab_sample = pd.read_sql(query_ab, conn)
    if not ab_sample.empty:
        # Get history for this well to show why it's AB
        file_no = ab_sample.iloc[0]['file_no']
        pool = ab_sample.iloc[0]['pool']
        print(f"Checking history for File No: {file_no}, Pool: {pool}")
        
        history_query = """
        SELECT date, bbls_oil, status 
        FROM production_data 
        WHERE file_no = ? AND pool = ? 
        ORDER BY date DESC 
        LIMIT 10
        """
        history = pd.read_sql(history_query, conn, params=(int(file_no), pool))
        print(history.to_string(index=False))
    else:
        print("No 'AB' status records found.")

    print("\n--- Sample Validation: Active Well (A) ---")
    query_a = """
    SELECT * FROM production_data 
    WHERE status = 'A' 
    LIMIT 1
    """
    a_sample = pd.read_sql(query_a, conn)
    if not a_sample.empty:
         # Get history for this well
        file_no = a_sample.iloc[0]['file_no']
        pool = a_sample.iloc[0]['pool']
        print(f"Checking history for File No: {file_no}, Pool: {pool}")
        
        history_query = """
        SELECT date, bbls_oil, status 
        FROM production_data 
        WHERE file_no = ? AND pool = ? 
        ORDER BY date DESC 
        LIMIT 5
        """
        history = pd.read_sql(history_query, conn, params=(int(file_no), pool))
        print(history.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    verify_status()
