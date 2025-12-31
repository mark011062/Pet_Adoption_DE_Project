import os
import pandas as pd
import uuid
from glob import glob

RAW_FOLDER = "raw"
CLEAN_FOLDER = "clean_parquet"

os.makedirs(CLEAN_FOLDER, exist_ok=True)

def save_clean(df, name):
    # Convert UUID columns to strings for Parquet compatibility
    for col in df.columns:
        if df[col].dtype == object and isinstance(df[col].iloc[0], uuid.UUID):
            df[col] = df[col].astype(str)
    csv_path = os.path.join(CLEAN_FOLDER, f"{name}.csv")
    parquet_path = os.path.join(CLEAN_FOLDER, f"{name}.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    print(f"✅ Saved cleaned {name}: CSV + Parquet")

def update_clean_csvs_db_aware():
    snapshots = sorted(glob(os.path.join(RAW_FOLDER, "animals_snapshot_*.csv")))
    if not snapshots:
        raise FileNotFoundError("No snapshots found in 'raw' folder")
    latest_snapshot = snapshots[-1]
    print(f"📥 Loading raw snapshot: {latest_snapshot}")
    pets_df = pd.read_csv(latest_snapshot)

    # --- Pet types ---
    types_df = pd.DataFrame(pets_df['type'].dropna().unique(), columns=['type'])
    types_df['type_id'] = [str(uuid.uuid4()) for _ in range(len(types_df))]
    save_clean(types_df[['type', 'type_id']], "pet_types")

    # --- Breeds ---
    breeds_df = pets_df[['type', 'breed']].drop_duplicates().dropna()
    breeds_df = breeds_df.merge(types_df, on='type', how='left')  # get type_id
    breeds_df['breed_id'] = [str(uuid.uuid4()) for _ in range(len(breeds_df))]
    save_clean(breeds_df[['breed', 'breed_id', 'type_id']], "breeds")

    # --- Shelters ---
    shelters_df = pets_df[['shelter_name', 'city', 'state']].drop_duplicates()
    shelters_df['shelter_id'] = [str(uuid.uuid4()) for _ in range(len(shelters_df))]
    save_clean(shelters_df[['shelter_name', 'city', 'state', 'shelter_id']], "shelters")

    # --- Pets ---
    # Merge in the IDs for type, breed, and shelter
    pets_clean = pets_df.merge(types_df, on='type', how='left')
    pets_clean = pets_clean.merge(breeds_df[['breed', 'type_id', 'breed_id']], on=['breed', 'type_id'], how='left')
    pets_clean = pets_clean.merge(shelters_df, on=['shelter_name', 'city', 'state'], how='left')

    # Keep only the columns we need
    pets_clean = pets_clean[['pet_id', 'name', 'age', 'gender', 'size', 'status',
                             'date_arrived', 'adopted_date', 'type_id', 'breed_id', 'shelter_id', 'snapshot_date']]

    save_clean(pets_clean, "pets")

if __name__ == "__main__":
    update_clean_csvs_db_aware()
    print("✅ Updated clean CSVs & Parquets: pet_types, breeds, shelters, pets")
