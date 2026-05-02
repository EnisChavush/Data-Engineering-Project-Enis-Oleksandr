import json
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

RULES_PATH = Path(__file__).resolve().parent.parent / "validation_rules" / "batch_rules.json"


def _load_rules(rules_path: Path = RULES_PATH) -> dict:
    try:
        with open(rules_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Validation rules file not found at: {rules_path}")


def validate(df: pd.DataFrame, rules_path: Path = RULES_PATH) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates df against the JSON rules file.

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

    missing_cols = [c for c in mandatory_cols if c not in df.columns]
    if missing_cols:
        logger.error(f"Dataset is missing mandatory columns: {missing_cols} — quarantining all rows")
        invalid_df = df.copy()
        invalid_df["_validation_errors"] = f"Dataset missing mandatory columns: {missing_cols}"
        return pd.DataFrame(columns=df.columns), invalid_df

    datetime_cols = {col for col, rule in col_rules.items() if rule.get("type") == "datetime" and col in df.columns}
    parsed_datetimes: dict[str, pd.Series] = {}
    for col in datetime_cols:
        parsed = pd.to_datetime(df[col], errors="coerce")
        flag(df[col].notna() & parsed.isna(), f"{col}: invalid datetime format")
        parsed_datetimes[col] = parsed

    for col in mandatory_cols:
        null_mask = df[col].isnull()
        if null_mask.any():
            flag(null_mask, f"{col}: mandatory column has null values")

    for col, rule in col_rules.items():
        if col not in df.columns:
            continue

        series = df[col]

        # not_null (already handled for mandatory; still flag for non-mandatory)
        if rule.get("not_null") and series.isnull().any():
            flag(series.isnull(), f"{col}: null value not allowed")

        if rule.get("type") in ("numeric", "integer"):
            numeric_series = pd.to_numeric(series, errors="coerce")
            flag(series.notna() & numeric_series.isna(), f"{col}: non-numeric value")
            if "min" in rule:
                flag(numeric_series.notna() & (numeric_series < rule["min"]),
                     f"{col}: value below minimum {rule['min']}")
            if "max" in rule:
                flag(numeric_series.notna() & (numeric_series > rule["max"]),
                     f"{col}: value above maximum {rule['max']}")

        if "allowed_values" in rule:
            bad = series.notna() & ~series.isin(rule["allowed_values"])
            flag(bad, f"{col}: value not in allowed set {rule['allowed_values']}")

        if rule.get("after_column") and rule["after_column"] in df.columns:
            col_dt = parsed_datetimes.get(col, pd.to_datetime(df[col], errors="coerce"))
            ref_dt = parsed_datetimes.get(rule["after_column"], pd.to_datetime(df[rule["after_column"]], errors="coerce"))
            both_valid = col_dt.notna() & ref_dt.notna()
            flag(both_valid & (col_dt <= ref_dt), f"{col}: dropoff is before or equal to pickup")

    invalid_df = df[invalid_mask].copy()
    invalid_df["_validation_errors"] = [
        "; ".join(failure_reasons.get(i, [])) for i in invalid_df.index
    ]
    valid_df = df[~invalid_mask].copy()

    total = len(df)
    pct = f"{len(invalid_df) / total * 100:.2f}%" if total > 0 else "N/A"
    logger.info(
        f"Validation complete – valid: {len(valid_df):,}, invalid: {len(invalid_df):,} "
        f"({pct} rejected)"
    )
    return valid_df, invalid_df