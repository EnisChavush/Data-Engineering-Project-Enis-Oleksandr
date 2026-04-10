"""
processor.py – Transforms the validated Yellow Taxi DataFrame.

Operations performed (as specified in the project brief):
  - Remove: VendorID, store_and_fwd_flag, RatecodeID
  - Add:    trip_duration_minutes, average_speed_mph, pickup_year, pickup_month,
            revenue_per_mile, trip_distance_category, fare_category, trip_time_of_day
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Columns to drop
COLUMNS_TO_DROP = ["VendorID", "store_and_fwd_flag", "RatecodeID"]


def process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformations and return the processed DataFrame.

    Args:
        df: Validated DataFrame.

    Returns:
        Processed DataFrame with new columns and without dropped columns.
    """
    df = df.copy()

    # ── 1. Remove specified columns (only those that exist) ───────────────────
    cols_to_drop = [c for c in COLUMNS_TO_DROP if c in df.columns]
    df.drop(columns=cols_to_drop, inplace=True)
    logger.info(f"Dropped columns: {cols_to_drop}")

    # ── 2. trip_duration_minutes ──────────────────────────────────────────────
    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds()
        / 60
    )

    # ── 3. average_speed_mph ──────────────────────────────────────────────────
    # Only compute where duration > 0; otherwise set to 0.
    df["average_speed_mph"] = 0.0
    valid_dur = df["trip_duration_minutes"] > 0
    df.loc[valid_dur, "average_speed_mph"] = (
        df.loc[valid_dur, "trip_distance"]
        / (df.loc[valid_dur, "trip_duration_minutes"] / 60)
    )

    # ── 4. pickup_year / pickup_month ─────────────────────────────────────────
    df["pickup_year"] = df["tpep_pickup_datetime"].dt.year
    df["pickup_month"] = df["tpep_pickup_datetime"].dt.month

    # ── 5. revenue_per_mile ───────────────────────────────────────────────────
    df["revenue_per_mile"] = 0.0
    valid_dist = df["trip_distance"] > 0
    df.loc[valid_dist, "revenue_per_mile"] = (
        df.loc[valid_dist, "total_amount"] / df.loc[valid_dist, "trip_distance"]
    )

    # ── 6. trip_distance_category ─────────────────────────────────────────────
    df["trip_distance_category"] = pd.cut(
        df["trip_distance"],
        bins=[-float("inf"), 2, 10, float("inf")],
        labels=["Short", "Medium", "Long"],
    )

    # ── 7. fare_category ─────────────────────────────────────────────────────
    df["fare_category"] = pd.cut(
        df["fare_amount"],
        bins=[-float("inf"), 20, 50, float("inf")],
        labels=["Low", "Medium", "High"],
    )

    # ── 8. trip_time_of_day ───────────────────────────────────────────────────
    hour = df["tpep_pickup_datetime"].dt.hour

    def _time_of_day(h: pd.Series) -> pd.Series:
        conditions = [
            (h >= 0) & (h < 6),    # Night
            (h >= 6) & (h < 12),   # Morning
            (h >= 12) & (h < 18),  # Afternoon
            (h >= 18) & (h < 24),  # Evening
        ]
        choices = ["Night", "Morning", "Afternoon", "Evening"]
        return pd.Series(
            pd.Categorical(
                pd.cut(h, bins=[-1, 5, 11, 17, 23], labels=choices),
                categories=choices
            ),
            index=h.index,
        )

    df["trip_time_of_day"] = _time_of_day(hour)

    logger.info(
        f"Processing complete – {len(df):,} rows, {len(df.columns)} columns."
    )
    return df