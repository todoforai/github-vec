import os

# Data directory configuration
# Set DATA_DIR env var to override (default: /home/root/data)
DATA_DIR = os.environ.get("DATA_DIR", "/home/root/data")

# Software Heritage snapshots (see dataset.md)
SWH_BUCKET = "s3://softwareheritage/graph"
SWH_SNAPSHOT_6K = "2023-09-06-popular-6k"
SWH_SNAPSHOT_FULL = "2025-11-28"

# Derived paths - ORC (raw from Software Heritage)
ORIGINS_6K_DIR = f"{DATA_DIR}/origins_6k"
ORIGINS_FULL_DIR = f"{DATA_DIR}/origins_full"
ORIGIN_VISIT_6K_DIR = f"{DATA_DIR}/origin_visit_6k"
ORIGIN_VISIT_FULL_DIR = f"{DATA_DIR}/origin_visit_full"

# Derived paths - Parquet (converted, faster)
ORIGINS_6K_PARQUET = f"{DATA_DIR}/github_origins_6k.parquet"
ORIGINS_FULL_PARQUET = f"{DATA_DIR}/github_origins_full.parquet"
VISITS_6K_PARQUET = f"{DATA_DIR}/github_visits_6k.parquet"
VISITS_FULL_PARQUET = f"{DATA_DIR}/github_visits_full.parquet"

READMES_DIR = f"{DATA_DIR}/readmes"
READMES_JSONL = f"{DATA_DIR}/readmes.jsonl"
