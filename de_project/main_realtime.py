import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.realtime_validator import validate_realtime
from pipeline.realtime_processor import process_realtime
from pipeline.writer import write_local, write_invalid_local

import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

print("=== Part 2: Real-Time Pipeline ===")

# Simulating a file being dropped in the input folder
file = Path(__file__).resolve().parent / "input" / "sales_transactions_dirty.csv"

# Step 1: Read
df = pd.read_csv(file)
print(f"Read {len(df)} rows from {file.name}")

# Step 2: Validate
valid_df, invalid_df = validate_realtime(df)

# Step 3: Process
processed_df = process_realtime(valid_df)

# Step 4: Write locally
write_local(processed_df, filename="sales_processed.parquet")
write_invalid_local(invalid_df, filename="sales_invalid.parquet")

print("=== Done! Check the output/ folder ===")