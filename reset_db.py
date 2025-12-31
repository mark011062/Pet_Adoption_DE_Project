# load/reset_db.py
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    dbname="pet_adoption",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

tables = ["animals", "breeds", "pet_types", "shelters"]

for table in tables:
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

# Recreate tables (adjust columns/types as per your loader)
cur.execute("""
CREATE TABLE pet_types (
    type_id UUID PRIMARY KEY,
    type TEXT NOT NULL
);
""")

cur.execute("""
CREATE TABLE breeds (
    breed_id UUID PRIMARY KEY,
    breed TEXT NOT NULL,
    type_id UUID REFERENCES pet_types(type_id)
);
""")

cur.execute("""
CREATE TABLE shelters (
    shelter_id UUID PRIMARY KEY,
    shelter_name TEXT NOT NULL,
    city TEXT,
    state TEXT
);
""")

cur.execute("""
CREATE TABLE animals (
    pet_id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    gender TEXT,
    size TEXT,
    status TEXT,
    date_arrived DATE,
    type_id UUID REFERENCES pet_types(type_id),
    breed_id UUID REFERENCES breeds(breed_id),
    shelter_id UUID REFERENCES shelters(shelter_id),
    snapshot_date DATE,
    adopted_date DATE
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Database reset complete")
