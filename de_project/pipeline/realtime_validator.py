import json
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

RULES_PATH = Path(__file__).resolve().parent.parent / "validation_rules" / "realtime_rules.json"


def _load_rules(rules_path: Path = RULES_PATH) -> dict:
    try:
        with open(rules_path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Validation rules file not found at: {rules_path}")


def validate_realtime(df: pd.DataFrame, rules_path: Path = RULES_PATH) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates a sales transaction DataFrame.

    Returns:
        (valid_df, invalid_df)
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

    missing = [c for c in mandatory_cols if c not in df.columns]
    if missing:
        logger.error(f"Missing mandatory columns: {missing} - quarantining all rows")
        invalid_df = df.copy()
        invalid_df["_validation_errors"] = f"Missing mandatory columns: {missing}"
        return pd.DataFrame(columns=df.columns), invalid_df

    for col in mandatory_cols:
        flag(df[col].isnull(), f"{col}: mandatory column has null value")

    for col, rule in col_rules.items():
        if col not in df.columns:
            continue
        series = df[col]

        if rule.get("unique"):
            flag(series.duplicated(keep="first"), f"{col}: duplicate value")

        if "allowed_values" in rule:
            flag(series.notna() & ~series.isin(rule["allowed_values"]),
                 f"{col}: not in allowed values {rule['allowed_values']}")

        if rule.get("type") in ("numeric", "integer"):
            if "min" in rule:
                flag(series.notna() & (pd.to_numeric(series, errors="coerce") < rule["min"]),
                     f"{col}: below minimum {rule['min']}")
            if "max" in rule:
                flag(series.notna() & (pd.to_numeric(series, errors="coerce") > rule["max"]),
                     f"{col}: above maximum {rule['max']}")

        if rule.get("type") == "datetime":
            parsed = pd.to_datetime(series, errors="coerce")
            flag(series.notna() & parsed.isna(), f"{col}: invalid datetime format")

        if rule.get("type") == "boolean":
            flag(series.notna() & ~series.isin([True, False, 0, 1]),
                 f"{col}: expected boolean value (True/False/0/1)")

    invalid_df = df[invalid_mask].copy()
    invalid_df["_validation_errors"] = [
        "; ".join(failure_reasons.get(i, [])) for i in invalid_df.index
    ]
    valid_df = df[~invalid_mask].copy()

    logger.info(
        f"Real-time validation – valid: {len(valid_df):,}, invalid: {len(invalid_df):,}"
    )
    return valid_df, invalid_df