import os
import pandas as pd

CLEAN_DIR = "clean_parquet"

required_columns = {
    "pet_types": ["type", "type_id"],
    "breeds": ["breed", "breed_id", "type_id"],
    "shelters": ["shelter_name", "city", "state", "shelter_id"],
    "pets": ["pet_id", "name", "age", "gender", "size", "status",
             "date_arrived", "adopted_date", "type_id", "breed_id", "shelter_id", "snapshot_date"]
}

all_passed = True

for csv_file, cols in required_columns.items():
    path = os.path.join(CLEAN_DIR, f"{csv_file}.csv")
    if not os.path.exists(path):
        print(f"❌ Missing CSV: {csv_file}.csv")
        all_passed = False
        continue

    df = pd.read_csv(path)
    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        print(f"❌ CSV {csv_file}.csv is missing columns: {missing_cols}")
        all_passed = False
    else:
        print(f"✅ {csv_file}.csv: {len(df)} rows, all required columns present")

if all_passed:
    print("\n🎉 All CSVs are valid!")
else:
    print("\n⚠️ Some CSVs are missing required columns or files.")
