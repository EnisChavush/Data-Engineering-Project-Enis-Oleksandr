import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.reader import read_parquet
from pipeline.validator import validate
from pipeline.processor import process
from pipeline.backup_validator import backup_validate
from pipeline.writer import write_all

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

print("=== Part 1: Batch Pipeline ===")

df = read_parquet(str(Path(__file__).resolve().parent / "yellow_tripdata_2025-01.parquet"))

valid_df, invalid_df = validate(df)

processed_df = process(valid_df)

clean_df, quarantine_df = backup_validate(processed_df)

results = write_all(clean_df, invalid_df, upload_to_azure=True)

print(f"Results: {results}")
print("=== Done! Check the output/ folder and Azure ===")