"""
transform/update_clean_csvs_db_aware.py

Option A (recommended):
- Process ONE raw snapshot at a time (explicitly chosen via --snapshot, or default = latest file)
- Produce normalized dimension tables + animals fact table in clean_parquet/ as CSV + Parquet
- Stamp snapshot_file into animals output so the loader can skip already-loaded snapshots

Outputs (clean_parquet/):
- pet_types.parquet  (type_id, type)
- breeds.parquet     (breed_id, breed, type_id)
- shelters.parquet   (shelter_id, shelter_name, city, state)
- animals.parquet    (pet_id, name, age, gender, size, status, date_arrived, adopted_date,
                      type_id, breed_id, shelter_id, snapshot_date, snapshot_file)

Run:
  python transform/update_clean_csvs_db_aware.py --snapshot raw/animals_snapshot_YYYYMMDD_HHMMSS.csv

Or (latest snapshot automatically):
  python transform/update_clean_csvs_db_aware.py
"""

import os
import uuid
import argparse
from glob import glob
from typing import Optional

import pandas as pd

RAW_FOLDER = "raw"
CLEAN_FOLDER = "clean_parquet"

os.makedirs(CLEAN_FOLDER, exist_ok=True)


# -----------------------------
# Helpers
# -----------------------------
def save_clean(df: pd.DataFrame, name: str) -> None:
    """Save DataFrame to both CSV and Parquet in CLEAN_FOLDER."""
    df = df.copy()

    csv_path = os.path.join(CLEAN_FOLDER, f"{name}.csv")
    parquet_path = os.path.join(CLEAN_FOLDER, f"{name}.parquet")

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"✅ Saved cleaned {name}: {csv_path} + {parquet_path} ({len(df)} rows)")


def _safe_to_date(series: pd.Series) -> pd.Series:
    """Parse a Series into python date objects (invalid -> NaT -> NaN)."""
    return pd.to_datetime(series, errors="coerce").dt.date


def _pick_snapshot(snapshot_path: Optional[str]) -> str:
    """Return the snapshot CSV path to process (explicit or latest)."""
    if snapshot_path:
        if not os.path.exists(snapshot_path):
            raise FileNotFoundError(f"Snapshot not found: {snapshot_path}")
        return snapshot_path

    snapshots = sorted(glob(os.path.join(RAW_FOLDER, "animals_snapshot_*.csv")))
    if not snapshots:
        raise FileNotFoundError("No snapshots found in 'raw' folder (expected raw/animals_snapshot_*.csv)")
    return snapshots[-1]


def _require_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    """Fail fast if a snapshot schema drifts."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Snapshot is missing required columns: {missing}")


def apply_data_quality_rules(pets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Curated-layer rules:
    - Normalize dates
    - Standardize text casing/spacing
    - Adopted_date implies status == 'Adopted'
    - If status == 'Adopted' and adopted_date missing, fill adopted_date with snapshot_date
    """
    pets_df = pets_df.copy()

    # Dates
    pets_df["date_arrived"] = _safe_to_date(pets_df["date_arrived"])
    pets_df["adopted_date"] = _safe_to_date(pets_df["adopted_date"])
    pets_df["snapshot_date"] = _safe_to_date(pets_df["snapshot_date"])

    # Strings (safe .astype(str) for mock data; if you later allow nulls, this still behaves)
    pets_df["status"] = pets_df["status"].astype(str).str.strip().str.title()
    pets_df["type"] = pets_df["type"].astype(str).str.strip().str.title()
    pets_df["breed"] = pets_df["breed"].astype(str).str.strip()
    pets_df["shelter_name"] = pets_df["shelter_name"].astype(str).str.strip()
    pets_df["city"] = pets_df["city"].astype(str).str.strip()
    pets_df["state"] = pets_df["state"].astype(str).str.strip()

    # If adopted_date is present, status must be Adopted
    conflict_mask = pets_df["adopted_date"].notna() & (pets_df["status"] != "Adopted")
    conflicts = int(conflict_mask.sum())
    if conflicts:
        print(
            f"⚠️ Data quality: {conflicts} rows had adopted_date but status != 'Adopted'. "
            f"Fixing status -> 'Adopted'."
        )
        pets_df.loc[conflict_mask, "status"] = "Adopted"

    # If status says Adopted but adopted_date is missing, fill with snapshot_date
    adopted_missing_date_mask = (pets_df["status"] == "Adopted") & (pets_df["adopted_date"].isna())
    adopted_missing = int(adopted_missing_date_mask.sum())
    if adopted_missing:
        print(
            f"⚠️ Data quality: {adopted_missing} rows had status='Adopted' but adopted_date missing. "
            f"Filling adopted_date with snapshot_date."
        )
        pets_df.loc[adopted_missing_date_mask, "adopted_date"] = pets_df.loc[
            adopted_missing_date_mask, "snapshot_date"
        ]

    # Types
    pets_df["age"] = pd.to_numeric(pets_df["age"], errors="coerce").astype("Int64")
    pets_df["pet_id"] = pets_df["pet_id"].astype(str).str.strip()

    return pets_df


# -----------------------------
# Main transform
# -----------------------------
def update_clean_csvs_db_aware(snapshot_path: Optional[str] = None) -> None:
    target_snapshot = _pick_snapshot(snapshot_path)
    snapshot_file = os.path.basename(target_snapshot)

    print(f"📥 Loading raw snapshot: {target_snapshot}")
    pets_df = pd.read_csv(target_snapshot)

    required_cols = [
        "pet_id", "name", "age", "gender", "size", "status",
        "date_arrived", "adopted_date",
        "type", "breed",
        "shelter_name", "city", "state",
        "snapshot_date",
    ]
    _require_columns(pets_df, required_cols)

    # Apply curated data-quality rules
    pets_df = apply_data_quality_rules(pets_df)

    # -----------------------------
    # Dimensions (built from THIS snapshot only)
    # -----------------------------
    types_df = (
        pd.DataFrame(pets_df["type"].dropna().unique(), columns=["type"])
        .sort_values("type")
        .reset_index(drop=True)
    )
    types_df["type_id"] = [str(uuid.uuid4()) for _ in range(len(types_df))]
    save_clean(types_df[["type", "type_id"]], "pet_types")

    breeds_df = pets_df[["type", "breed"]].drop_duplicates().dropna()
    breeds_df = (
        breeds_df.merge(types_df, on="type", how="left")
        .sort_values(["type", "breed"])
        .reset_index(drop=True)
    )
    breeds_df["breed_id"] = [str(uuid.uuid4()) for _ in range(len(breeds_df))]
    save_clean(breeds_df[["breed", "breed_id", "type_id"]], "breeds")

    shelters_df = (
        pets_df[["shelter_name", "city", "state"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    shelters_df["shelter_id"] = [str(uuid.uuid4()) for _ in range(len(shelters_df))]
    save_clean(shelters_df[["shelter_name", "city", "state", "shelter_id"]], "shelters")

    # -----------------------------
    # Fact table: animals (THIS snapshot only)
    # -----------------------------
    animals_df = pets_df.merge(types_df, on="type", how="left")
    animals_df = animals_df.merge(
        breeds_df[["breed", "type_id", "breed_id"]],
        on=["breed", "type_id"],
        how="left",
    )
    animals_df = animals_df.merge(
        shelters_df,
        on=["shelter_name", "city", "state"],
        how="left",
    )

    animals_df = animals_df[
        [
            "pet_id",
            "name",
            "age",
            "gender",
            "size",
            "status",
            "date_arrived",
            "adopted_date",
            "type_id",
            "breed_id",
            "shelter_id",
            "snapshot_date",
        ]
    ].copy()

    # IMPORTANT: stamp snapshot filename for load tracking (Option B)
    animals_df["snapshot_file"] = snapshot_file

    save_clean(animals_df, "animals")

    print("✅ Updated clean CSVs & Parquets: pet_types, breeds, shelters, animals")
    print(f"🧾 Snapshot stamped for tracking: {snapshot_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        help="Path to a specific raw snapshot CSV to process (Option A). If omitted, uses latest snapshot in raw/.",
        default=None,
    )
    args = parser.parse_args()
    update_clean_csvs_db_aware(args.snapshot)
