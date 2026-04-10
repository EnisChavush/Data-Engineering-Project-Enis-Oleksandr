"""
reader.py – Reads the Yellow Taxi Trip Records parquet file into a DataFrame.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def read_parquet(file_path: str) -> pd.DataFrame:
    """
    Read a parquet file and return a DataFrame.

    Args:
        file_path: Path to the .parquet file.

    Returns:
        Raw DataFrame with all original columns.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file cannot be parsed as parquet.
    """
    logger.info(f"Reading parquet file: {file_path}")
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Successfully read {len(df):,} rows and {len(df.columns)} columns.")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to read parquet file: {e}")
        raise ValueError(f"Cannot parse parquet file '{file_path}': {e}") from e