# \# Pet Adoption Data Engineering Project

# 

# This project simulates a data engineering pipeline for pet adoption data. It supports multiple snapshots, incremental updates, and loads normalized data into PostgreSQL.

# 

# ---

# 

# \## \*\*Folder Structure\*\*

# 

# Pet\_Adoption\_DE\_Project/

# ├── raw/ # Raw CSV snapshots (immutable)

# ├── clean/ # Cleaned CSVs ready for processing

# ├── clean\_parquet/ # Normalized Parquet tables for loading

# ├── ingest/ # Scripts to generate or ingest raw snapshots

# ├── transform/ # Scripts to clean and prepare data

# ├── load/ # Scripts to load data into PostgreSQL

# ├── config/ # Configuration files (DB connection, etc.)

# └── tests/ # Optional tests

# 

# yaml

# Copy code

# 

# ---

# 

# \## \*\*Workflow\*\*

# 

# \### 1. Generate a new raw snapshot

# ```bash

# python -m ingest.generate\_mock\_animals\_csv

# Generates a new CSV in raw/ with randomized date\_arrived and optional adopted\_date.

# 

# 2\. Clean and normalize data

# bash

# Copy code

# python transform/update\_clean\_csvs\_db\_aware.py

# Reads all snapshots in raw/

# 

# Produces cleaned CSVs in clean/

# 

# Produces normalized Parquet files in clean\_parquet/

# 

# Tables updated: pet\_types, breeds, shelters, pets/animals.

# 

# 3\. Load data incrementally into PostgreSQL

# bash

# Copy code

# python load/load\_parquet\_to\_postgres\_incremental\_v3.py

# Ensures all tables exist

# 

# Loads new rows incrementally into: pet\_types, breeds, shelters, animals

# 

# Notes

# Multiple snapshots: The pipeline handles multiple raw snapshots automatically.

# 

# Incremental loading: New data is merged into the existing database without overwriting existing rows.

# 

# UUIDs: All primary/foreign keys are UUIDs to ensure referential integrity.

# 

# Randomized dates: date\_arrived is randomized per pet; adopted\_date is optional.

# 

# Database Schema Overview

# pet\_types → type\_id, type

# 

# breeds → breed\_id, breed, type\_id

# 

# shelters → shelter\_id, shelter\_name, city, state

# 

# animals → pet\_id, name, age, gender, size, status, date\_arrived, adopted\_date, type\_id, breed\_id, shelter\_id, snapshot\_date

# 

# Quick Start

# Activate virtual environment:

# 

# bash

# Copy code

# source .venv/bin/activate  # Linux/Mac

# .venv\\Scripts\\activate     # Windows

# Install dependencies:

# 

# bash

# Copy code

# pip install -r requirements.txt

# Run the full workflow:

# 

# bash

# Copy code

# python -m ingest.generate\_mock\_animals\_csv

# python transform/update\_clean\_csvs\_db\_aware.py

# python load/load\_parquet\_to\_postgres\_incremental\_v3.py

# Contact / Support

# For issues with the pipeline or database errors, ensure that:

# 

# Raw snapshots exist in raw/

# 

# Cleaned CSVs and Parquets exist in clean/ and clean\_parquet/

# 

# PostgreSQL is running and accessible with correct credentials in config/db\_config.py

