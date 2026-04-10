"""
validator.py – Validates the Yellow Taxi DataFrame against the rules defined in
validation_rules/batch_rules.json.

Returns a tuple of (valid_df, invalid_df) so the pipeline can write invalid
rows to a quarantine file for investigation.
"""

import json
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

RULES_PATH = Path(__file__).resolve().parent.parent / "validation_rules" / "batch_rules.json"


def _load_rules(rules_path: Path = RULES_PATH) -> dict:
    with open(rules_path, "r") as f:
        return json.load(f)


def validate(df: pd.DataFrame, rules_path: Path = RULES_PATH) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate *df* against the JSON rules file.

    Args:
        df:          Raw DataFrame from the reader.
        rules_path:  Path to the JSON rules file.

    Returns:
        (valid_df, invalid_df) – rows that passed / failed validation.
    """
    rules = _load_rules(rules_path)
    mandatory_cols = rules["mandatory_columns"]
    col_rules = rules["rules"]

    invalid_mask = pd.Series(False, index=df.index)
    failure_reasons: dict[int, list[str]] = {}

    def flag(mask: pd.Series, reason: str):
        for idx in df.index[mask]:
            failure_reasons.setdefault(idx, []).append(reason)
        nonlocal invalid_mask
        invalid_mask |= mask

    # ── 1. Mandatory columns present ──────────────────────────────────────────
    missing_cols = [c for c in mandatory_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Dataset is missing mandatory columns: {missing_cols}")

    # ── 2. Mandatory columns not-null ─────────────────────────────────────────
    for col in mandatory_cols:
        null_mask = df[col].isnull()
        if null_mask.any():
            flag(null_mask, f"{col}: mandatory column has null values")

    # ── 3. Column-level rules ─────────────────────────────────────────────────
    for col, rule in col_rules.items():
        if col not in df.columns:
            continue

        series = df[col]

        # not_null (already handled for mandatory; still flag for non-mandatory)
        if rule.get("not_null") and series.isnull().any():
            flag(series.isnull(), f"{col}: null value not allowed")

        # numeric bounds
        if rule.get("type") == "numeric" and col in df.columns:
            non_null = series.dropna()
            if "min" in rule:
                flag(series.notna() & (series < rule["min"]),
                     f"{col}: value below minimum {rule['min']}")
            if "max" in rule:
                flag(series.notna() & (series > rule["max"]),
                     f"{col}: value above maximum {rule['max']}")

        # integer allowed_values
        if "allowed_values" in rule:
            bad = series.notna() & ~series.isin(rule["allowed_values"])
            flag(bad, f"{col}: value not in allowed set {rule['allowed_values']}")

        # integer bounds (PULocationID / DOLocationID)
        if rule.get("type") == "integer" and col in df.columns:
            if "min" in rule:
                flag(series.notna() & (series < rule["min"]),
                     f"{col}: value below minimum {rule['min']}")
            if "max" in rule:
                flag(series.notna() & (series > rule["max"]),
                     f"{col}: value above maximum {rule['max']}")

        # datetime ordering
        if rule.get("after_column") and rule["after_column"] in df.columns:
            bad_order = df[col] <= df[rule["after_column"]]
            flag(bad_order, f"{col}: dropoff is not after pickup")

    # ── 4. Build output DataFrames ────────────────────────────────────────────
    invalid_df = df[invalid_mask].copy()
    invalid_df["_validation_errors"] = [
        "; ".join(failure_reasons.get(i, [])) for i in invalid_df.index
    ]
    valid_df = df[~invalid_mask].copy()

    logger.info(
        f"Validation complete – valid: {len(valid_df):,}, invalid: {len(invalid_df):,} "
        f"({len(invalid_df)/len(df)*100:.2f}% rejected)"
    )
    return valid_df, invalid_df