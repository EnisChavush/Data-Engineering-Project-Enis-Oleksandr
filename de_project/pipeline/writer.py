"""
writer.py – Writes the processed DataFrame to:
  1. A local output folder (parquet)
  2. Azure Blob Storage

Azure credentials are read from environment variables:
  AZURE_STORAGE_CONNECTION_STRING  (preferred)
  or AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY

Set AZURE_CONTAINER_NAME to override the default container name.
"""

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CONTAINER = "yellow-taxi-processed"
LOCAL_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


# ── Local writer ──────────────────────────────────────────────────────────────

def write_local(df: pd.DataFrame, filename: str = "yellow_taxi_processed.parquet") -> Path:
    """
    Write DataFrame to the local output folder as parquet.

    Args:
        df:       Processed DataFrame.
        filename: Output filename.

    Returns:
        Absolute path to the written file.
    """
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOCAL_OUTPUT_DIR / filename
    df.to_parquet(out_path, index=False)
    logger.info(f"Local write complete: {out_path}  ({len(df):,} rows)")
    return out_path


def write_invalid_local(df: pd.DataFrame, filename: str = "yellow_taxi_invalid.parquet") -> Path:
    """Write rejected rows to the local output folder."""
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOCAL_OUTPUT_DIR / filename
    df.to_parquet(out_path, index=False)
    logger.info(f"Invalid rows written locally: {out_path}  ({len(df):,} rows)")
    return out_path


# ── Azure Blob writer ─────────────────────────────────────────────────────────

def _get_blob_service_client():
    """Build an Azure BlobServiceClient from environment variables."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        raise ImportError(
            "azure-storage-blob is not installed. "
            "Run: pip install azure-storage-blob"
        )

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    if account_name and account_key:
        return BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

    raise EnvironmentError(
        "Azure credentials not found. Set AZURE_STORAGE_CONNECTION_STRING "
        "or AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY."
    )


def write_azure(
    df: pd.DataFrame,
    blob_name: str = "yellow_taxi_processed.parquet",
    container_name: str | None = None,
) -> str:
    """
    Upload the processed DataFrame to Azure Blob Storage as parquet.

    Args:
        df:             Processed DataFrame.
        blob_name:      Name of the blob (file) in Azure.
        container_name: Target container (defaults to env var AZURE_CONTAINER_NAME
                        or DEFAULT_CONTAINER).

    Returns:
        Full blob URL.
    """
    container = container_name or os.getenv("AZURE_CONTAINER_NAME", DEFAULT_CONTAINER)

    client = _get_blob_service_client()

    # Ensure container exists
    container_client = client.get_container_client(container)
    try:
        container_client.create_container()
        logger.info(f"Created Azure container: {container}")
    except Exception:
        pass  # Container already exists

    # Serialise to parquet in-memory
    import io
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, overwrite=True)

    url = blob_client.url
    logger.info(f"Azure upload complete: {url}  ({len(df):,} rows)")
    return url


# ── Combined writer ───────────────────────────────────────────────────────────

def write_all(
    valid_df: pd.DataFrame,
    invalid_df: pd.DataFrame,
    upload_to_azure: bool = True,
) -> dict:
    """
    Write valid and invalid DataFrames to all destinations.

    Args:
        valid_df:        Processed, clean rows.
        invalid_df:      Rejected rows (from both validators).
        upload_to_azure: If False, skip Azure upload (useful for local testing).

    Returns:
        Dict with keys 'local_valid', 'local_invalid', and optionally 'azure_url'.
    """
    result = {}
    result["local_valid"] = write_local(valid_df)
    result["local_invalid"] = write_invalid_local(invalid_df)

    if upload_to_azure:
        try:
            result["azure_url"] = write_azure(valid_df)
        except EnvironmentError as e:
            logger.warning(f"Azure upload skipped: {e}")
        except Exception as e:
            logger.error(f"Azure upload failed: {e}")
            raise

    return result