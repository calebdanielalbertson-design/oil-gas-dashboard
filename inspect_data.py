import pandas as pd
import os

files = [
    r"C:\Users\User\Documents\DMR production 2024.xlsx",
    r"C:\Users\User\Documents\DMR production 2025 oct.xlsx"
]

for f in files:
    print(f"--- Inspecting {os.path.basename(f)} ---")
    try:
        # Read only a few rows to get headers and types
        df = pd.read_excel(f, nrows=5)
        print("Columns:")
        for col in df.columns:
            print(f"  - {col} ({df[col].dtype})")
        print("\nSample Data:")
        print(df.head(2).to_string())
        print("\n" + "="*30 + "\n")
    except Exception as e:
        print(f"Error reading {f}: {e}")
