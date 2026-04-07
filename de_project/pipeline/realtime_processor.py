"""
realtime_processor.py – Processes validated sales transaction DataFrames.

Adds at least 3 new columns and removes duplicates, as required by Part 2.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def process_realtime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a validated sales transaction DataFrame.

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

    # ── Remove duplicates ─────────────────────────────────────────────────────
    before = len(df)
    df.drop_duplicates(subset=["transaction_id"], keep="first", inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info(f"Duplicates removed: {before - len(df)} rows dropped.")

    # ── Ensure timestamp is datetime ──────────────────────────────────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ── New column 1: revenue_after_discount ──────────────────────────────────
    discount = df["discount_pct"].fillna(0)
    df["revenue_after_discount"] = (df["total_price"] * (1 - discount / 100)).round(2)

    # ── New column 2: price_tier ──────────────────────────────────────────────
    df["price_tier"] = pd.cut(
        df["unit_price"],
        bins=[-float("inf"), 20, 100, float("inf")],
        labels=["Budget", "Mid-range", "Premium"],
    )

    # ── New column 3: transaction_hour ────────────────────────────────────────
    df["transaction_hour"] = df["timestamp"].dt.hour

    # ── New column 4: is_high_value ───────────────────────────────────────────
    df["is_high_value"] = df["total_price"] > 200

    # ── New column 5: days_since_epoch ────────────────────────────────────────
    epoch = pd.Timestamp("2025-01-01")
    df["days_since_epoch"] = (df["timestamp"] - epoch).dt.days

    logger.info(f"Real-time processing complete – {len(df):,} rows, {len(df.columns)} columns.")
    return df