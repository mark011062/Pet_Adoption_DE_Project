"""
load_raw_csv.py

Purpose:
--------
Simulate ingestion of a raw CSV snapshot.
- Reads the raw CSV
- Saves it as an immutable snapshot partitioned by date
"""

import os
import pandas as pd
from datetime import date

# ---------------------------------------------------------
# Step 1: Define paths
# ---------------------------------------------------------
RAW_CSV_PATH = "raw/animals_snapshot.csv"  # Original CSV
SNAPSHOT_BASE_DIR = "raw/animals"          # Where snapshots will live

# ---------------------------------------------------------
# Step 2: Load CSV into a pandas DataFrame
# ---------------------------------------------------------
try:
    df = pd.read_csv(RAW_CSV_PATH)
    print(f"✅ Loaded {len(df)} records from {RAW_CSV_PATH}")
except FileNotFoundError:
    raise FileNotFoundError(f"{RAW_CSV_PATH} not found. Make sure the CSV exists.")

# ---------------------------------------------------------
# Step 3: Save as a snapshot partitioned by date
# ---------------------------------------------------------
today = date.today().isoformat()  # e.g., "2025-12-29"
snapshot_dir = os.path.join(SNAPSHOT_BASE_DIR, f"dt={today}")
os.makedirs(snapshot_dir, exist_ok=True)

snapshot_path = os.path.join(snapshot_dir, "animals.csv")
df.to_csv(snapshot_path, index=False)

print(f"✅ Saved snapshot to {snapshot_path}")
