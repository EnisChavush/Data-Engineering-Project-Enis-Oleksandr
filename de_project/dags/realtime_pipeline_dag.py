"""
realtime_pipeline_dag.py – Airflow DAG for Real-Time Sales Data Processing (Part 2).

Uses a FileSensor to monitor an input folder.
When a new .csv or .xlsx file lands, the full pipeline runs:
  Sensor → Reader → Validator → Processor → Backup Validator → Writer

The processed file is moved to a "processed" sub-folder so the sensor
doesn't re-trigger on the same file.
"""

import logging
import shutil
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.realtime_validator import validate_realtime
from pipeline.realtime_processor import process_realtime
from pipeline.writer import write_all

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_DIR = Path(Variable.get(
    "REALTIME_INPUT_DIR",
    default_var=str(Path(__file__).resolve().parent.parent / "input"),
))
UPLOAD_TO_AZURE = Variable.get("UPLOAD_TO_AZURE", default_var="true").lower() == "true"

INPUT_DIR.mkdir(parents=True, exist_ok=True)
(INPUT_DIR / "processed").mkdir(exist_ok=True)

default_args = {
    "owner": "de_student",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_new_file() -> Path | None:
    """Return the first unprocessed .csv or .xlsx file in the input folder."""
    for ext in ("*.csv", "*.xlsx"):
        files = sorted(INPUT_DIR.glob(ext))
        for f in files:
            if f.parent.name != "processed":
                return f
    return None


def _read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    elif path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


# ── Task functions ────────────────────────────────────────────────────────────

def task_detect_and_read(**context):
    file_path = _find_new_file()
    if file_path is None:
        raise FileNotFoundError("No new .csv or .xlsx file found in input folder.")

    logger.info(f"Processing file: {file_path}")
    df = _read_file(file_path)
    logger.info(f"Read {len(df):,} rows from {file_path.name}")

    tmp = Path("/tmp/rt_raw.parquet")
    df.to_parquet(tmp, index=False)
    context["ti"].xcom_push(key="raw_path", value=str(tmp))
    context["ti"].xcom_push(key="source_file", value=str(file_path))


def task_validate(**context):
    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="reader")
    df = pd.read_parquet(raw_path)

    valid_df, invalid_df = validate_realtime(df)

    valid_tmp = Path("/tmp/rt_valid.parquet")
    invalid_tmp = Path("/tmp/rt_invalid.parquet")
    valid_df.to_parquet(valid_tmp, index=False)
    invalid_df.to_parquet(invalid_tmp, index=False)

    context["ti"].xcom_push(key="valid_path", value=str(valid_tmp))
    context["ti"].xcom_push(key="invalid_path", value=str(invalid_tmp))


def task_process(**context):
    valid_path = context["ti"].xcom_pull(key="valid_path", task_ids="validator")
    df = pd.read_parquet(valid_path)

    processed_df = process_realtime(df)

    processed_tmp = Path("/tmp/rt_processed.parquet")
    processed_df.to_parquet(processed_tmp, index=False)
    context["ti"].xcom_push(key="processed_path", value=str(processed_tmp))


def task_backup_validate(**context):
    processed_path = context["ti"].xcom_pull(key="processed_path", task_ids="processor")
    invalid_path = context["ti"].xcom_pull(key="invalid_path", task_ids="validator")
    df = pd.read_parquet(processed_path)

    # Basic post-processing checks
    issues = pd.Series(False, index=df.index)
    reasons: dict[int, list[str]] = {}

    def flag(mask, reason):
        for i in df.index[mask]:
            reasons.setdefault(i, []).append(reason)
        nonlocal issues
        issues |= mask

    for col in ["revenue_after_discount", "transaction_hour", "days_since_epoch"]:
        if col not in df.columns:
            raise RuntimeError(f"Expected column missing after processing: {col}")

    flag(df["transaction_hour"] < 0, "transaction_hour is negative")
    flag(df["transaction_hour"] > 23, "transaction_hour > 23")
    flag(df["revenue_after_discount"] < 0, "revenue_after_discount is negative")

    quarantine_df = df[issues].copy()
    quarantine_df["_backup_errors"] = ["; ".join(reasons.get(i, [])) for i in quarantine_df.index]
    clean_df = df[~issues].copy()

    existing_invalid = pd.read_parquet(invalid_path)
    all_invalid = pd.concat([existing_invalid, quarantine_df], ignore_index=True)

    clean_tmp = Path("/tmp/rt_clean.parquet")
    invalid_final_tmp = Path("/tmp/rt_invalid_final.parquet")
    clean_df.to_parquet(clean_tmp, index=False)
    all_invalid.to_parquet(invalid_final_tmp, index=False)

    context["ti"].xcom_push(key="clean_path", value=str(clean_tmp))
    context["ti"].xcom_push(key="invalid_final_path", value=str(invalid_final_tmp))


def task_write(**context):
    clean_path = context["ti"].xcom_pull(key="clean_path", task_ids="backup_validator")
    invalid_path = context["ti"].xcom_pull(key="invalid_final_path", task_ids="backup_validator")
    source_file = context["ti"].xcom_pull(key="source_file", task_ids="reader")

    clean_df = pd.read_parquet(clean_path)
    invalid_df = pd.read_parquet(invalid_path)

    # Use the source filename (without extension) for outputs
    stem = Path(source_file).stem
    results = write_all(
        valid_df=clean_df,
        invalid_df=invalid_df,
        upload_to_azure=UPLOAD_TO_AZURE,
    )
    logger.info(f"Write results: {results}")

    # Move source file to processed sub-folder
    dest = INPUT_DIR / "processed" / Path(source_file).name
    shutil.move(source_file, dest)
    logger.info(f"Moved source file to: {dest}")


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="realtime_sales_pipeline",
    default_args=default_args,
    description="Part 2: Real-Time Sales Data Processing with Folder Monitoring",
    schedule_interval=timedelta(minutes=1),   # Poll every minute
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "sales", "de-project"],
) as dag:

    # Sensor waits for any .csv or .xlsx file in the input folder
    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath=str(INPUT_DIR / "*.csv"),   # Airflow FileSensor uses glob
        fs_conn_id="fs_default",
        poke_interval=30,         # Check every 30 seconds
        timeout=60 * 60,          # Give up after 1 hour
        mode="poke",
    )

    reader = PythonOperator(
        task_id="reader",
        python_callable=task_detect_and_read,
    )

    validator = PythonOperator(
        task_id="validator",
        python_callable=task_validate,
    )

    processor = PythonOperator(
        task_id="processor",
        python_callable=task_process,
    )

    backup_validator = PythonOperator(
        task_id="backup_validator",
        python_callable=task_backup_validate,
    )

    writer = PythonOperator(
        task_id="writer",
        python_callable=task_write,
    )

    file_sensor >> reader >> validator >> processor >> backup_validator >> writer