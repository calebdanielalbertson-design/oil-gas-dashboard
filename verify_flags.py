import sqlite3
import pandas as pd

DB_NAME = "production.db"

def verify_flags():
    print(f"Connecting to {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    
    # Check if columns exist
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(production_data)")
    cols = [info[1] for info in cursor.fetchall()]
    
    if 'no_prod_1m' in cols and 'no_prod_2m' in cols:
        print("Success: Columns 'no_prod_1m' and 'no_prod_2m' found found.")
    else:
        print("Error: Columns missing!")
        print(f"Current columns: {cols}")
        return

    print("\n--- Flag Counts ---")
    query = """
    SELECT 
        no_prod_1m, 
        no_prod_2m, 
        count(*) as count 
    FROM production_data 
    GROUP BY 1, 2 
    ORDER BY 1, 2
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    print("\n--- Cross-Validation with Status ---")
    # Verify that if no_prod_2m is 1, status implies inactivity (unless manually overridden logic issue?)
    # Actually, logic says: 2m overrides 1m, 3m overrides 2m, etc.
    # So if no_prod_2m is 1, status could be 'IA 2 - A' OR 'IA' OR 'AB' (since 3m/6m also satisfy 2m condition implicitly? 
    # Wait, rolling(2).sum()==2.
    # If 3 months dry: [0,0,0]. 
    # Month 2: [0,0] -> 2m=True. 
    # Month 3: [0,0] -> 2m=True AND 3m=True.
    # So yes, 2m flag can be true for IA and AB as well.
    
    query_val = """
    SELECT status, count(*) as count 
    FROM production_data 
    WHERE no_prod_2m = 1 
    GROUP BY 1 
    ORDER BY 2 DESC
    """
    print("\nStatus distribution where no_prod_2m = 1:")
    val_df = pd.read_sql(query_val, conn)
    print(val_df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    verify_flags()
