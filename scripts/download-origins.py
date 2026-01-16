#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path
from config import (
    DATA_DIR, SWH_BUCKET,
    SWH_SNAPSHOT_6K, SWH_SNAPSHOT_FULL,
    ORIGINS_6K_DIR, ORIGINS_FULL_DIR,
    ORIGIN_VISIT_6K_DIR, ORIGIN_VISIT_FULL_DIR
)

parser = argparse.ArgumentParser(description="Download Software Heritage origins ORC files")
parser.add_argument("--full", action="store_true", help="Download full dataset (~16GB origins, ~42GB visits) instead of 6k popular repos")
args = parser.parse_args()

if args.full:
    snapshot = SWH_SNAPSHOT_FULL
    origins_dir = ORIGINS_FULL_DIR
    visits_dir = ORIGIN_VISIT_FULL_DIR
    size_note = "(Full dataset: ~16GB origins + ~42GB visits)"
else:
    snapshot = SWH_SNAPSHOT_6K
    origins_dir = ORIGINS_6K_DIR
    visits_dir = ORIGIN_VISIT_6K_DIR
    size_note = "(6k popular repos: ~253KB origins + ~5MB visits)"

print("=== Software Heritage Origins Download ===")
print(f"Snapshot: {snapshot}")
print(f"Data dir: {DATA_DIR}")
print(f"Origins dir: {origins_dir}")
print(f"Visits dir: {visits_dir}")
print()

# Ensure directories exist
Path(origins_dir).mkdir(parents=True, exist_ok=True)
Path(visits_dir).mkdir(parents=True, exist_ok=True)

# Check for aws cli
try:
    subprocess.run(["which", "aws"], check=True, capture_output=True)
except subprocess.CalledProcessError:
    print("ERROR: aws cli not found. Install with: snap install aws-cli --classic")
    sys.exit(1)

# Download origin ORC files
s3_origins = f"{SWH_BUCKET}/{snapshot}/orc/origin/"
print(f"[1/2] Downloading origins from {s3_origins}...")
print(size_note)
print()

subprocess.run([
    "aws", "s3", "sync",
    s3_origins,
    f"{origins_dir}/",
    "--no-sign-request"
], check=True)

# Download origin_visit ORC files
s3_visits = f"{SWH_BUCKET}/{snapshot}/orc/origin_visit/"
print()
print(f"[2/2] Downloading origin_visit from {s3_visits}...")
print()

subprocess.run([
    "aws", "s3", "sync",
    s3_visits,
    f"{visits_dir}/",
    "--no-sign-request"
], check=True)

print()
print("Download complete. Verifying with PyArrow...")

# Import here to avoid circular import and allow verification
from db import count_github_origins
try:
    count = count_github_origins(origins_dir)
    print()
    print("=== Complete ===")
    print(f"GitHub origins: {count:,}")
    print(f"Origins ORC: {origins_dir}/")
    print(f"Visits ORC: {visits_dir}/")
except Exception as e:
    print(f"Error reading ORC files: {e}")
    sys.exit(1)
