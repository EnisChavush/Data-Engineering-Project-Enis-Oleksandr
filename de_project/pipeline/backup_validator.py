import logging
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

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
        # accumulating all rows that have been tagged as quarantined
        invalid_mask |= mask

    still_present = [c for c in EXPECTED_DROPPED if c in df.columns]
    if still_present:
        logger.warning(f"Columns that should have been dropped are still present: {still_present}")

    # Quarantine all rows rather than crashing the pipeline if the processor skipped a column
    missing_new = [c for c in EXPECTED_NEW_COLUMNS if c not in df.columns]
    if missing_new:
        logger.error(f"Processor did not create expected columns: {missing_new} - quarantining all rows")
        quarantine_df = df.copy()
        quarantine_df["_backup_validation_errors"] = f"Missing processor output columns: {missing_new}"
        return pd.DataFrame(columns=df.columns), quarantine_df

    # checking for whether the column values are null because 
    # NaN < 0 evaluates to False
    flag(df["trip_duration_minutes"].isnull(), "trip_duration_minutes is null")
    flag(df["trip_duration_minutes"] < 0, "trip_duration_minutes is negative")

    flag(df["average_speed_mph"].isnull(), "average_speed_mph is null")
    flag(df["average_speed_mph"] < 0, "average_speed_mph is negative")
    flag(df["average_speed_mph"] > 200, "average_speed_mph exceeds 200 mph (likely data error)")

    flag(~df["pickup_year"].between(2000, 2100), "pickup_year out of range 2000–2100")
    flag(~df["pickup_month"].between(1, 12), "pickup_month out of range 1–12")

    flag(df["revenue_per_mile"].isnull(), "revenue_per_mile is null")
    flag(df["revenue_per_mile"] < 0, "revenue_per_mile is negative")

    flag(df["trip_distance_category"].isnull(), "trip_distance_category is null")
    dist_str = df["trip_distance_category"].astype(str)
    flag(df["trip_distance_category"].notna() & ~dist_str.isin(DISTANCE_CATEGORIES),
         "trip_distance_category has unexpected label")

    flag(df["fare_category"].isnull(), "fare_category is null")
    fare_str = df["fare_category"].astype(str)
    flag(df["fare_category"].notna() & ~fare_str.isin(FARE_CATEGORIES),
         "fare_category has unexpected label")

    flag(df["trip_time_of_day"].isnull(), "trip_time_of_day is null")
    tod_str = df["trip_time_of_day"].astype(str)
    flag(df["trip_time_of_day"].notna() & ~tod_str.isin(TIME_OF_DAY_CATEGORIES),
         "trip_time_of_day has unexpected label")

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