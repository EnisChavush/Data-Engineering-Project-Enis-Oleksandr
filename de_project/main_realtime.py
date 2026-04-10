import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.realtime_validator import validate_realtime
from pipeline.realtime_processor import process_realtime
from pipeline.writer import write_all

import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

print("=== Part 2: Real-Time Pipeline ===")

file = Path(__file__).resolve().parent / "input" / "sales_transactions_dirty.csv"

df = pd.read_csv(file)
print(f"Read {len(df)} rows from {file.name}")

valid_df, invalid_df = validate_realtime(df)

processed_df = process_realtime(valid_df)

results = write_all(
    processed_df, 
    invalid_df, 
    upload_to_azure=True,
)

print(f"Results: {results}")
print("=== Done! Check the output/ folder and Azure ===")