import logging

import pandas as pd

logger = logging.getLogger(__name__)


def process_realtime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a validated sales transaction DataFrame.

    New columns added:
      - revenue_after_discount : total_price adjusted for discount
      - price_tier             : "Budget" / "Mid-range" / "Premium"
      - transaction_hour       : hour of day (0-23) from timestamp
      - is_high_value          : True when total_price > 200
      - days_since_epoch        : days since 2025-01-01 (useful for trend analysis)

    Also removes duplicate transaction_id rows.

    Args:
        df: Validated sales DataFrame.

    Returns:
        Processed DataFrame.
    """
    df = df.copy()

    len_before = len(df)
    df.drop_duplicates(subset=["transaction_id"], keep="first", inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info(f"Duplicates removed: {len_before - len(df)} rows dropped.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    discount = df["discount_pct"].fillna(0)
    df["revenue_after_discount"] = (df["total_price"] * (1 - discount / 100)).round(2)

    df["price_tier"] = pd.cut(
        df["unit_price"],
        bins=[-float("inf"), 20, 100, float("inf")],
        labels=["Budget", "Mid-range", "Premium"],
    )

    df["transaction_hour"] = df["timestamp"].dt.hour

    df["is_high_value"] = df["total_price"] > 200

    epoch = pd.Timestamp("2025-01-01")
    df["days_since_epoch"] = (df["timestamp"] - epoch).dt.days

    logger.info(f"Real-time processing complete – {len(df):,} rows, {len(df.columns)} columns.")
    return df