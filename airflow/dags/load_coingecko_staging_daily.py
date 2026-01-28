import logging
import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from sqlalchemy import create_engine

OWNER = "n.znak"
DAG_ID = "load_coingecko_staging_daily"

BUCKET = "prod"
LAYER = "raw"
SOURCE = "coingecko"

# MinIO
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default_var="aZzwPOxDKLSbA4SJmxjH")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default_var="hrh9KUgEoVkE2MKOCkAexPH023M3ZCqaohZ8VwPh")
MINIO_ENDPOINT = Variable.get("minio_endpoint", default_var="minio:9000")

# Postgres
POSTGRES_USER = Variable.get("dwh_postgres_user", default_var="postgres")
POSTGRES_PASSWORD = Variable.get("dwh_postgres_password", default_var="postgres")
POSTGRES_DB = Variable.get("dwh_postgres_db", default_var="crypto_dwh")
POSTGRES_HOST = Variable.get("dwh_postgres_host", default_var="postgres-dwh")
POSTGRES_PORT = Variable.get("dwh_postgres_port", default_var="5432")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 26, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

def get_load_date(**context) -> str:
    return context["data_interval_start"].format("YYYY-MM-DD")


def load_s3_to_staging(**context):
    load_date = get_load_date(**context)
    logging.info(f"💻 Start staging load for date: {load_date}")

    # Подключение к Postgres через SQLAlchemy
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    # Путь к Parquet в S3
    s3_path = f"s3://{BUCKET}/raw/coingecko/daily/{load_date}/{load_date}.parquet"

    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": f"http://{MINIO_ENDPOINT}"},
    }

    # Чтение parquet
    logging.info(f"📥 Reading Parquet from S3: {s3_path}")
    df = pd.read_parquet(s3_path, engine="pyarrow", storage_options=storage_options)
    df["load_date"] = load_date

    n_rows = len(df)
    logging.info(f"📊 Rows loaded from S3: {n_rows}")

    if n_rows == 0:
        raise ValueError("❌ No rows found in Parquet file")

    table_name = "stg_coingecko_markets"
    logging.info(f"📤 Writing data to Postgres table {table_name}")

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(".", "_")
    )

    df.to_sql(
        table_name,
        engine,
        schema="staging",
        index=False,
        if_exists="append",
        method="multi",
        chunksize=500,
    )

    logging.info(f"✅ Loaded {n_rows} rows into {table_name}")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["coingecko", "staging", "postgres"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="load_coingecko_raw_daily",
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,
        poke_interval=60,
    )

    load_to_staging = PythonOperator(
        task_id="load_s3_to_staging",
        python_callable=load_s3_to_staging,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> load_to_staging >> end
