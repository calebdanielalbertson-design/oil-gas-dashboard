import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

DB_NAME = "production.db"
OUTPUT_IMAGE = "monthly_records.png"

def plot_monthly_coverage():
    print(f"Connecting to {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    
    query = """
    SELECT 
        strftime('%Y-%m', date) as month,
        COUNT(*) as record_count
    FROM production_data
    WHERE date IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """
    
    print("Querying data...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Convert month to datetime for better plotting
    df['month'] = pd.to_datetime(df['month'])
    
    print(f"Data retrieved: {len(df)} months found.")
    
    # Plotting
    plt.figure(figsize=(15, 6))
    plt.plot(df['month'], df['record_count'], marker='.', linestyle='-', markersize=2)
    
    plt.title('Number of Production Records per Month (2000 - 2025)')
    plt.xlabel('Date')
    plt.ylabel('Record Count')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Highlight potential gaps (e.g., if count drops to 0 or varies wildly)
    # For now, just a clear visual line is enough.
    
    plt.tight_layout()
    
    # Save
    save_path = os.path.abspath(OUTPUT_IMAGE)
    plt.savefig(save_path)
    print(f"Plot saved to: {save_path}")

if __name__ == "__main__":
    plot_monthly_coverage()
