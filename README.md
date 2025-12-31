\# Pet Adoption DE Project - Full Workflow



\# 1. Activate environment

\# Linux/Mac:

source .venv/bin/activate

\# Windows:

.venv\\Scripts\\activate



\# 2. Install dependencies

pip install -r requirements.txt



\# 3. Generate raw snapshot

python -m ingest.generate\_mock\_animals\_csv



\# 4. Clean \& normalize data

python transform/update\_clean\_csvs\_db\_aware.py



\# 5. Load incrementally into PostgreSQL

python load/load\_parquet\_to\_postgres\_incremental\_v3.py



\# Notes:

\# - Multiple snapshots handled automatically

\# - Incremental loading preserves existing rows

\# - UUIDs for all primary/foreign keys

\# - Randomized date\_arrived; adopted\_date optional



\# Folder structure:

\# Pet\_Adoption\_DE\_Project/

\# ├── raw/               # Raw CSV snapshots

\# ├── clean\_parquet/     # Normalized Parquet tables

\# ├── ingest/            # Snapshot generation scripts

\# ├── transform/         # Cleaning / normalization scripts

\# ├── load/              # Load into PostgreSQL

\# ├── config/            # DB configs

\# └── tests/             # Optional tests



\# Tables:

\# pet\_types → type\_id, type

\# breeds → breed\_id, breed, type\_id

\# shelters → shelter\_id, shelter\_name, city, state

\# animals → pet\_id, name, age, gender, size, status, date\_arrived, adopted\_date, type\_id, breed\_id, shelter\_id, snapshot\_date



\# Requirements:

\# - raw/ snapshots exist

\# - clean/ and clean\_parquet/ contain processed data

\# - PostgreSQL running with credentials in config/db\_config.py

