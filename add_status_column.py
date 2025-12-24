import pandas as pd
import sqlite3
import os

DB_NAME = "production.db"

def add_status_column():
    print(f"Connecting to {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    
    print("Reading data into DataFrame...")
    # Read necessary columns + sorting columns
    # We need everything to write back the full table with the new column
    df = pd.read_sql("SELECT * FROM production_data", conn)
    conn.close()
    
    print(f"Loaded {len(df)} rows. Sorting...")
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by=['file_no', 'pool', 'date'], inplace=True)
    
    print("Calculating Status (this may take a moment)...")

    def apply_status_logic(group):
        # Rolling checks for zero production
        # rolling().sum() == window_size implies all in window are 0 (since we check == 0)
        is_zero_prod = (group['bbls_oil'] == 0).astype(int)
        
        no_prod_1m = is_zero_prod.rolling(window=1).sum() == 1
        no_prod_2m = is_zero_prod.rolling(window=2).sum() == 2
        no_prod_3m = is_zero_prod.rolling(window=3).sum() == 3
        no_prod_6m = is_zero_prod.rolling(window=6).sum() == 6
        
        is_active = group['bbls_oil'] > 0
        
        # We can assign directly to the group slices
        # Default status
        group['status'] = 'Unknown'
        
        # Apply logic in order (later overwrites earlier)
        group.loc[no_prod_1m, 'status'] = 'IA 1 - A'
        group.loc[no_prod_2m, 'status'] = 'IA 2 - A'
        group.loc[no_prod_3m, 'status'] = 'IA'
        group.loc[no_prod_6m, 'status'] = 'AB'
        group.loc[is_active, 'status'] = 'A'
        
        return group

    # GroupBy apply is flexible but can be slow on 3M rows. 
    # Optimization: Vectorized operations on the whole sorted dataframe with checks for group boundaries.
    # However, pandas rolling supports 'groupby' efficiently in newer versions.
    # Let's try the direct groupby-apply on small function or transform.
    # Groupby.apply might be too slow for 3M rows if groups are small (many wells).
    # Faster approach: calculates rolling on whole DF, then mask out where FileNo/Pool changes.
    
    # Let's stick to the user's logic structure but optimize if needed. 
    # With 3M rows, a simple apply might take 5-10 mins.
    # Vectorized approach:
    
    # 1. Calculate is_zero_prod global
    df['is_zero'] = (df['bbls_oil'] == 0).astype(int)
    
    # 2. Groupby Rolling
    # This is efficient in modern pandas
    g = df.groupby(['file_no', 'pool'])['is_zero']
    
    # We need to map the result back to the index
    print("  - Calculating rolling windows...")
    # Calculate boolean masks
    mask_1m = g.rolling(window=1).sum().reset_index(level=[0,1], drop=True) == 1
    mask_2m = g.rolling(window=2).sum().reset_index(level=[0,1], drop=True) == 2
    mask_3m = g.rolling(window=3).sum().reset_index(level=[0,1], drop=True) == 3
    mask_6m = g.rolling(window=6).sum().reset_index(level=[0,1], drop=True) == 6
    
    # Assign new persistent columns (0 or 1)
    print("  - Assigning flag columns...")
    df['no_prod_1m'] = mask_1m.astype(int)
    df['no_prod_2m'] = mask_2m.astype(int)

    print("  - Assigning statuses...")
    df['status'] = 'Unknown'
    
    # Align indices (reset_index above might have messed alignment if sorting changed, but we sorted inplace)
    # The result of groupby().rolling() return values matched the df order because we didn't drop keys in g = df.groupby...
    # Wait, reset_index(drop=True) on the result of rolling() removes the MultiIndex (FileNo, Pool, original_index).
    # If the original dataframe index was not monotonic increasing or had gaps, reset_index(drop=True) might align purely by position.
    # Since we sorted `df` by FileNo, Pool, Date before grouping, and groupby preserves that order, positional alignment is safe 
    # IF the rolling operation output one row per input row (which it does).
    
    # Apply logic in order (later overwrites earlier)
    # We use the boolean masks directly
    df.loc[mask_1m, 'status'] = 'IA 1 - A'
    df.loc[mask_2m, 'status'] = 'IA 2 - A'
    df.loc[mask_3m, 'status'] = 'IA'
    df.loc[mask_6m, 'status'] = 'AB'
    
    # Active overrides all (e.g. if this month produced, it is Active, even if rolling sum says 0? 
    # Wait, if this month produced(>0), then rolling sum of 0s cannot be == window size, 
    # UNLESS window size is large and we look at past? 
    # No, rolling is looking at [t-window+1, t]. If t produced, then sum of 0s is at most window-1.
    # So 'is_active' and 'no_prod_Xm' are mutually exclusive for the calculated frame.
    
    # However, let's strictly follow the user logic:
    # "Immediately set Status to 'Active' if BBLS_OIL_COND > 0 for any period"
    # This implies checking the current row value.
    is_active = df['bbls_oil'] > 0
    df.loc[is_active, 'status'] = 'A'
    
    # Remove helper column if any
    if 'is_zero' in df.columns:
        df.drop(columns=['is_zero'], inplace=True)
        
    print("Writing back to database...")
    conn = sqlite3.connect(DB_NAME)
    df.to_sql('production_data', conn, if_exists='replace', index=False)
    
    print("Re-creating indices...")
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_no ON production_data (api_no)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON production_data (date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON production_data (status)")
    # Index the new flags for efficient analytics
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_np1m ON production_data (no_prod_1m)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_np2m ON production_data (no_prod_2m)")
    conn.commit()
    conn.close()
    
    print("--- Status Column Added ---")

if __name__ == "__main__":
    add_status_column()
