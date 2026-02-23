import json
import logging
import requests
import duckdb
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "n.znak"
DAG_ID = "load_coingecko_raw_daily"

LAYER = "raw"
SOURCE = "coingecko"
BUCKET = "prod"

ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var="aZzwPOxDKLSbA4SJmxjH")
SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var="hrh9KUgEoVkE2MKOCkAexPH023M3ZCqaohZ8VwPh")

COINS = [
    "bitcoin", "ethereum", "solana", "cardano", "tron",
    "chainlink", "tether", "hyperliquid", "stellar", "monero"
]
API_KEY = Variable.get("COINGECKO_API_KEY", default_var="CG-3CdQVAQ59QkQrKzSiLVutAod")

LONG_DESCRIPTION = """
# DAG: Load daily crypto data from CoinGecko into MinIO

This DAG performs the following steps:
1. Fetches daily market data for 10 selected cryptocurrencies from the CoinGecko API.
2. Normalizes the data and adds a load date.
3. Writes the resulting dataset in Parquet format to MinIO/S3 via DuckDB.
4. Logs the number of records processed for monitoring.
"""

SHORT_DESCRIPTION = "Fetch daily CoinGecko market data and store in MinIO"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 2, 22, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

def get_dates(**context) -> str:
    return context["data_interval_start"].format("YYYY-MM-DD")


def get_and_transfer_coingecko_data_to_s3(**context):
    load_date = get_dates(**context)
    logging.info(f"💻 Start load for date: {load_date}")

    url = "https://api.coingecko.com/api/v3/coins/markets"
    headers = {
        "x-cg-demo-api-key": API_KEY,
        "accept": "application/json"
    }
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d",
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not data:
        raise ValueError("❌ CoinGecko API returned empty response")

    logging.info(f"📥 Records fetched from API: {len(data)}")
    logging.info(json.dumps(data, indent=2))

    df = pd.json_normalize(data)
    df["load_date"] = load_date

    n_rows = len(df)
    logging.info(f"📊 Rows after Pandas load: {n_rows}")

    if n_rows == 0:
        raise ValueError("❌ No rows after JSON parsing")

    context["ti"].xcom_push(key="rows_count", value=n_rows)

    object_path = f"s3://{BUCKET}/{LAYER}/{SOURCE}/daily/{load_date}/{load_date}.parquet"
    con = duckdb.connect()
    try:
        con.sql(
            f"""
                SET TIMEZONE='UTC';
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_url_style = 'path';
                SET s3_endpoint = 'minio:9000';
                SET s3_access_key_id = '{ACCESS_KEY}';
                SET s3_secret_access_key = '{SECRET_KEY}';
                SET s3_use_ssl = FALSE;
                """
        )
        con.execute(f"COPY df TO '{object_path}' (FORMAT PARQUET);")
        logging.info(f"✅ Data written to {object_path}")
    finally:
        con.close()

    logging.info(f"🏁 CoinGecko load finished for {load_date}")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["coingecko", "raw", "s3"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    get_and_transfer_data = PythonOperator(
        task_id="get_and_transfer_coingecko_data_to_s3",
        python_callable=get_and_transfer_coingecko_data_to_s3,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> get_and_transfer_data >> end
