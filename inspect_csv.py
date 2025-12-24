import pandas as pd

FILE_PATH = r"C:\Users\User\Desktop\Production data 1-2000_-_12-2023v2 Update Status.csv"

try:
    # Read just the header
    df = pd.read_csv(FILE_PATH, nrows=5)
    print("Columns found in CSV:")
    for col in df.columns:
        print(f"  - {col}")
except Exception as e:
    print(f"Error reading CSV: {e}")
