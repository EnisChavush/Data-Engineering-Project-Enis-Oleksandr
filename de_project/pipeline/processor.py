import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Columns to drop
COLUMNS_TO_DROP = ["VendorID", "store_and_fwd_flag", "RatecodeID"]


def process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies all transformations and returns the processed DataFrame.

    Args:
        df: Validated DataFrame.

    Returns:
        Processed DataFrame with new columns and without dropped columns.
    """
    df = df.copy()

    cols_to_drop = [c for c in COLUMNS_TO_DROP if c in df.columns]
    df.drop(columns=cols_to_drop, inplace=True)
    logger.info(f"Dropped columns: {cols_to_drop}")

    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds()
        / 60
    )

    df["average_speed_mph"] = 0.0
    valid_dur = df["trip_duration_minutes"] > 0 # only compute average speed if duration > 0
    df.loc[valid_dur, "average_speed_mph"] = (
        df.loc[valid_dur, "trip_distance"]
        / (df.loc[valid_dur, "trip_duration_minutes"] / 60)
    )

    df["pickup_year"] = df["tpep_pickup_datetime"].dt.year
    df["pickup_month"] = df["tpep_pickup_datetime"].dt.month

    df["revenue_per_mile"] = 0.0
    valid_dist = df["trip_distance"] > 0
    df.loc[valid_dist, "revenue_per_mile"] = (
        df.loc[valid_dist, "total_amount"] / df.loc[valid_dist, "trip_distance"]
    )

    df["trip_distance_category"] = pd.cut(
        df["trip_distance"],
        bins=[-float("inf"), 2, 10, float("inf")],
        labels=["Short", "Medium", "Long"],
    )

    df["fare_category"] = pd.cut(
        df["fare_amount"],
        bins=[-float("inf"), 20, 50, float("inf")],
        labels=["Low", "Medium", "High"],
    )

    hour = df["tpep_pickup_datetime"].dt.hour

    def _time_of_day(h: pd.Series) -> pd.Series:
        categories = ["Night", "Morning", "Afternoon", "Evening"]
        return pd.Series(
            pd.Categorical(
                pd.cut(h, bins=[-1, 5, 11, 17, 23], labels=categories),
                categories=categories
            ),
            index=h.index,
        )

    df["trip_time_of_day"] = _time_of_day(hour)

    logger.info(
        f"Processing complete – {len(df):,} rows, {len(df.columns)} columns."
    )
    return df