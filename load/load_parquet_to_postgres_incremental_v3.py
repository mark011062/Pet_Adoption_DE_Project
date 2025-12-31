import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import uuid

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pet_adoption",
    "user": "postgres",
    "password": "password"
}

CLEAN_FOLDER = "clean_parquet"

TABLES = {
    "pet_types": "pet_types",
    "breeds": "breeds",
    "shelters": "shelters",
    "pets": "animals"
}

def load_parquet(file_name):
    path = os.path.join(CLEAN_FOLDER, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing Parquet: {path}")
    return pd.read_parquet(path)

def upsert_table(cur, table_name, df, conflict_cols):
    if df.empty:
        return

    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    columns_str = ", ".join(cols)
    conflict_str = ", ".join(conflict_cols)
    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols])

    sql = f"""
    INSERT INTO {table_name} ({columns_str})
    VALUES ({placeholders})
    ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};
    """
    execute_batch(cur, sql, df.itertuples(index=False, name=None))

def load_incremental():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print("⚡ Ensuring all tables exist and loading data incrementally...")

    # Load tables in dependency order
    pet_types_df = load_parquet("pet_types.parquet")
    upsert_table(cur, TABLES["pet_types"], pet_types_df, ["type_id"])
    print(f"✅ Loaded {len(pet_types_df)} rows into pet_types")

    breeds_df = load_parquet("breeds.parquet")
    upsert_table(cur, TABLES["breeds"], breeds_df, ["breed_id"])
    print(f"✅ Loaded {len(breeds_df)} rows into breeds")

    shelters_df = load_parquet("shelters.parquet")
    upsert_table(cur, TABLES["shelters"], shelters_df, ["shelter_id"])
    print(f"✅ Loaded {len(shelters_df)} rows into shelters")

    pets_df = load_parquet("pets.parquet")
    upsert_table(cur, TABLES["pets"], pets_df, ["pet_id"])
    print(f"✅ Loaded {len(pets_df)} rows into animals")

    cur.close()
    conn.close()
    print("🎉 All tables loaded successfully!")

if __name__ == "__main__":
    load_incremental()
