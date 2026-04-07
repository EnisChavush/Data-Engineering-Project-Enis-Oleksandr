import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.reader import read_parquet
from pipeline.validator import validate
from pipeline.processor import process
from pipeline.backup_validator import backup_validate
from pipeline.writer import write_local, write_invalid_local

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

print("=== Part 1: Batch Pipeline ===")

# Step 1: Read
df = read_parquet(str(Path(__file__).resolve().parent / "yellow_tripdata_2025-01.parquet"))

# Step 2: Validate
valid_df, invalid_df = validate(df)

# Step 3: Process
processed_df = process(valid_df)

# Step 4: Backup validate
clean_df, quarantine_df = backup_validate(processed_df)

# Step 5: Write locally (skipping Azure for now)
write_local(clean_df)
write_invalid_local(invalid_df)

print("=== Done! Check the output/ folder ===")