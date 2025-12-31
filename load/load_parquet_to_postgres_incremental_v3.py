import os
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

CLEAN_FOLDER = "clean_parquet"

TABLES = {
    "pet_types": "pet_types",
    "breeds": "breeds",
    "shelters": "shelters",
    "animals": "animals",
}


def get_db_config() -> dict:
    cfg = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "pet_adoption"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD"),
    }
    if not cfg["password"]:
        raise ValueError("Missing PGPASSWORD. Set it in your environment or in a .env file.")
    return cfg


def load_parquet(file_name: str) -> pd.DataFrame:
    path = os.path.join(CLEAN_FOLDER, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing Parquet: {path}")
    return pd.read_parquet(path)


def ensure_loaded_snapshots_table(cur) -> None:
    """
    Ensure the tracking table exists with the schema you chose (Choice 1).
    NOTE: This will not modify an existing table; it only creates if missing.
    """
    cur.execute("""
    CREATE TABLE IF NOT EXISTS loaded_snapshots (
        snapshot_file TEXT PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)


def snapshot_already_loaded(cur, snapshot_file: str) -> bool:
    cur.execute("SELECT 1 FROM loaded_snapshots WHERE snapshot_file = %s;", (snapshot_file,))
    return cur.fetchone() is not None


def mark_snapshot_loaded(cur, snapshot_file: str, snapshot_date) -> None:
    """
    Record that a snapshot file has been successfully loaded.
    snapshot_date is required (NOT NULL) by your table.
    """
    cur.execute(
        """
        INSERT INTO loaded_snapshots (snapshot_file, snapshot_date)
        VALUES (%s, %s)
        ON CONFLICT (snapshot_file) DO NOTHING;
        """,
        (snapshot_file, snapshot_date)
    )


def ensure_str_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if col.endswith("_id") or col == "pet_id":
            df[col] = df[col].astype(str)
    return df


def df_to_python_records(df: pd.DataFrame) -> list[tuple]:
    """
    Convert numpy scalar types (numpy.int64, numpy.float64, etc.) into native Python
    so psycopg2 can adapt them. Also convert NaN/NaT -> None for proper SQL NULLs.
    """
    df = df.copy()
    df = df.where(pd.notnull(df), None)

    records: list[tuple] = []
    for row in df.itertuples(index=False, name=None):
        clean_row = []
        for v in row:
            if isinstance(v, np.generic):
                v = v.item()
            clean_row.append(v)
        records.append(tuple(clean_row))
    return records


def upsert_table(cur, table_name: str, df: pd.DataFrame, conflict_cols: list[str]) -> None:
    if df.empty:
        print(f"⚠️ Skipping {table_name}: dataframe is empty")
        return

    df = ensure_str_ids(df)

    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    columns_str = ", ".join(cols)
    conflict_str = ", ".join(conflict_cols)

    update_cols = [c for c in cols if c not in conflict_cols]

    if not update_cols:
        sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_str}) DO NOTHING;
        """
    else:
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};
        """

    records = df_to_python_records(df)
    execute_batch(cur, sql, records, page_size=1000)


def load_incremental() -> None:
    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cur = conn.cursor()

    # Make sure tracking table exists with correct schema
    ensure_loaded_snapshots_table(cur)

    # Read animals first to identify snapshot_file + snapshot_date
    animals_df = load_parquet("animals.parquet")

    # Validate snapshot_file
    if "snapshot_file" not in animals_df.columns:
        raise ValueError("animals.parquet is missing 'snapshot_file'. Re-run transform with the updated script.")

    snapshot_files = animals_df["snapshot_file"].dropna().unique().tolist()
    if len(snapshot_files) != 1:
        raise ValueError(f"Expected exactly 1 snapshot_file in animals.parquet, found: {snapshot_files}")

    snapshot_file = snapshot_files[0]

    # Validate snapshot_date (required by loaded_snapshots schema)
    if "snapshot_date" not in animals_df.columns:
        raise ValueError("animals.parquet is missing 'snapshot_date'.")

    snapshot_dates = animals_df["snapshot_date"].dropna().unique().tolist()
    if len(snapshot_dates) != 1:
        raise ValueError(f"Expected exactly 1 snapshot_date in animals.parquet, found: {snapshot_dates}")

    snapshot_date = snapshot_dates[0]

    # Skip if already loaded
    if snapshot_already_loaded(cur, snapshot_file):
        print(f"⏭️ Snapshot already loaded, skipping: {snapshot_file}")
        cur.close()
        conn.close()
        return

    print(f"⚡ Loading dimension tables + animals (incremental upsert) for snapshot: {snapshot_file}")

    # Load dimensions
    pet_types_df = load_parquet("pet_types.parquet")
    upsert_table(cur, TABLES["pet_types"], pet_types_df, ["type_id"])
    print(f"✅ Loaded {len(pet_types_df)} rows into pet_types")

    breeds_df = load_parquet("breeds.parquet")
    upsert_table(cur, TABLES["breeds"], breeds_df, ["breed_id"])
    print(f"✅ Loaded {len(breeds_df)} rows into breeds")

    shelters_df = load_parquet("shelters.parquet")
    upsert_table(cur, TABLES["shelters"], shelters_df, ["shelter_id"])
    print(f"✅ Loaded {len(shelters_df)} rows into shelters")

    # Insert snapshot_file too (animals table now includes it)
    upsert_table(cur, TABLES["animals"], animals_df, ["pet_id"])
    print(f"✅ Loaded {len(animals_df)} rows into animals")


    # Record successful load with snapshot_date (NOT NULL)
    mark_snapshot_loaded(cur, snapshot_file, snapshot_date)
    print(f"🧾 Recorded loaded snapshot: {snapshot_file} ({snapshot_date})")

    cur.close()
    conn.close()
    print("🎉 Snapshot load complete!")


if __name__ == "__main__":
    load_incremental()
