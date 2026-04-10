"""
backup_validator.py – Post-processing validation.

Runs after the Processor to confirm that:
  - All newly added columns are present and contain no unexpected nulls.
  - Derived numeric columns have sensible ranges.
  - Categorical columns only contain expected labels.

If problems are found, rows are quarantined rather than crashing the pipeline.
"""

import logging
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Expected new columns after processing
EXPECTED_NEW_COLUMNS = [
    "trip_duration_minutes",
    "average_speed_mph",
    "pickup_year",
    "pickup_month",
    "revenue_per_mile",
    "trip_distance_category",
    "fare_category",
    "trip_time_of_day",
]

EXPECTED_DROPPED = ["VendorID", "store_and_fwd_flag", "RatecodeID"]

DISTANCE_CATEGORIES = {"Short", "Medium", "Long"}
FARE_CATEGORIES = {"Low", "Medium", "High"}
TIME_OF_DAY_CATEGORIES = {"Night", "Morning", "Afternoon", "Evening"}


def backup_validate(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate the processed DataFrame.

    Returns:
        (clean_df, quarantine_df) – rows that passed / failed post-processing checks.
    """
    invalid_mask = pd.Series(False, index=df.index)
    failure_reasons: dict[int, list[str]] = {}

    def flag(mask: pd.Series, reason: str):
        for idx in df.index[mask]:
            failure_reasons.setdefault(idx, []).append(reason)
        nonlocal invalid_mask
        invalid_mask |= mask

    # ── 1. Dropped columns are gone ───────────────────────────────────────────
    still_present = [c for c in EXPECTED_DROPPED if c in df.columns]
    if still_present:
        logger.warning(f"Columns that should have been dropped are still present: {still_present}")

    # ── 2. New columns exist ──────────────────────────────────────────────────
    missing_new = [c for c in EXPECTED_NEW_COLUMNS if c not in df.columns]
    if missing_new:
        raise RuntimeError(f"Processor did not create expected columns: {missing_new}")

    # ── 3. trip_duration_minutes >= 0 ─────────────────────────────────────────
    flag(df["trip_duration_minutes"] < 0, "trip_duration_minutes is negative")

    # ── 4. average_speed_mph >= 0 and not unreasonably high ──────────────────
    flag(df["average_speed_mph"] < 0, "average_speed_mph is negative")
    flag(df["average_speed_mph"] > 200, "average_speed_mph exceeds 200 mph (likely data error)")

    # ── 5. pickup_year / pickup_month plausible ───────────────────────────────
    flag(~df["pickup_year"].between(2000, 2100), "pickup_year out of range 2000–2100")
    flag(~df["pickup_month"].between(1, 12), "pickup_month out of range 1–12")

    # ── 6. revenue_per_mile >= 0 ──────────────────────────────────────────────
    flag(df["revenue_per_mile"] < 0, "revenue_per_mile is negative")

    # ── 7. Categorical columns have valid labels ───────────────────────────────
    dist_str = df["trip_distance_category"].astype(str)
    flag(~dist_str.isin(DISTANCE_CATEGORIES | {"nan"}),
         "trip_distance_category has unexpected label")

    fare_str = df["fare_category"].astype(str)
    flag(~fare_str.isin(FARE_CATEGORIES | {"nan"}),
         "fare_category has unexpected label")

    tod_str = df["trip_time_of_day"].astype(str)
    flag(~tod_str.isin(TIME_OF_DAY_CATEGORIES | {"nan"}),
         "trip_time_of_day has unexpected label")

    # ── 8. Build output DataFrames ────────────────────────────────────────────
    quarantine_df = df[invalid_mask].copy()
    quarantine_df["_backup_validation_errors"] = [
        "; ".join(failure_reasons.get(i, [])) for i in quarantine_df.index
    ]
    clean_df = df[~invalid_mask].copy()

    logger.info(
        f"Backup validation complete – clean: {len(clean_df):,}, "
        f"quarantined: {len(quarantine_df):,}"
    )
    return clean_df, quarantine_df