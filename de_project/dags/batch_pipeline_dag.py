import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.reader import read_parquet
from pipeline.validator import validate
from pipeline.processor import process
from pipeline.backup_validator import backup_validate
from pipeline.writer import write_all

logger = logging.getLogger(__name__)

DATA_PATH = Variable.get(
    "TAXI_DATA_PATH",
    default_var=str(Path(__file__).resolve().parent.parent / "yellow_tripdata_2025-01.parquet"),
)

UPLOAD_TO_AZURE = Variable.get("UPLOAD_TO_AZURE", default_var="true").lower() == "true"

default_args = {
    "owner": "de_student",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def task_read(**context):
    df = read_parquet(DATA_PATH)
    tmp = Path("/tmp/taxi_raw.parquet")
    df.to_parquet(tmp, index=False)
    context["ti"].xcom_push(key="raw_path", value=str(tmp))
    logger.info(f"Reader finished. Rows: {len(df):,}")


def task_validate(**context):
    import pandas as pd
    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="reader")
    df = pd.read_parquet(raw_path)
    valid_df, invalid_df = validate(df)
    valid_tmp = Path("/tmp/taxi_valid.parquet")
    invalid_tmp = Path("/tmp/taxi_invalid.parquet")
    valid_df.to_parquet(valid_tmp, index=False)
    invalid_df.to_parquet(invalid_tmp, index=False)
    context["ti"].xcom_push(key="valid_path", value=str(valid_tmp))
    context["ti"].xcom_push(key="invalid_path", value=str(invalid_tmp))


def task_process(**context):
    valid_path = context["ti"].xcom_pull(key="valid_path", task_ids="validator")
    df = pd.read_parquet(valid_path)
    processed_df = process(df)
    processed_tmp = Path("/tmp/taxi_processed.parquet")
    processed_df.to_parquet(processed_tmp, index=False)
    context["ti"].xcom_push(key="processed_path", value=str(processed_tmp))


def task_backup_validate(**context):
    processed_path = context["ti"].xcom_pull(key="processed_path", task_ids="processor")
    invalid_path = context["ti"].xcom_pull(key="invalid_path", task_ids="validator")
    df = pd.read_parquet(processed_path)
    clean_df, quarantine_df = backup_validate(df)
    existing_invalid = pd.read_parquet(invalid_path)
    all_invalid = pd.concat([existing_invalid, quarantine_df], ignore_index=True)
    clean_tmp = Path("/tmp/taxi_clean.parquet")
    invalid_tmp = Path("/tmp/taxi_invalid_final.parquet")
    clean_df.to_parquet(clean_tmp, index=False)
    all_invalid.to_parquet(invalid_tmp, index=False)
    context["ti"].xcom_push(key="clean_path", value=str(clean_tmp))
    context["ti"].xcom_push(key="invalid_final_path", value=str(invalid_tmp))


def task_write(**context):
    clean_path = context["ti"].xcom_pull(key="clean_path", task_ids="backup_validator")
    invalid_path = context["ti"].xcom_pull(key="invalid_final_path", task_ids="backup_validator")
    clean_df = pd.read_parquet(clean_path)
    invalid_df = pd.read_parquet(invalid_path)
    results = write_all(clean_df, invalid_df, upload_to_azure=UPLOAD_TO_AZURE)
    logger.info(f"Write results: {results}")


with DAG(
    dag_id="yellow_taxi_batch_pipeline",
    default_args=default_args,
    description="Yellow Taxi Batch Processing Pipeline",
    schedule_interval="0,5,10,15,20,25,30,35,40,45,50,55 10 4 5 *",
    start_date=datetime(2026, 5, 3),
    catchup=False,
    tags=["batch", "yellow-taxi", "de-project"],
) as dag:

    reader = PythonOperator(task_id="reader", python_callable=task_read)
    validator = PythonOperator(task_id="validator", python_callable=task_validate)
    processor = PythonOperator(task_id="processor", python_callable=task_process)
    backup_validator = PythonOperator(task_id="backup_validator", python_callable=task_backup_validate)
    writer = PythonOperator(task_id="writer", python_callable=task_write)

    reader >> validator >> processor >> backup_validator >> writer