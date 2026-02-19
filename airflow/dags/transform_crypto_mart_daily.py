import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.docker.operators.docker import DockerOperator

OWNER = "n.znak"
DAG_ID = "transform_crypto_mart_daily"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 26, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["dbt", "mart", "transform"],
) as dag:

    start = EmptyOperator(task_id="start")

    sensor_on_staging_layer = ExternalTaskSensor(
        task_id="sensor_on_staging_layer",
        external_dag_id="load_coingecko_staging_daily",
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=36000,
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="crypto-data-project-dbt",
        command="dbt run",
        working_dir="/dbt",
        network_mode="crypto-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        mount_tmp_dir=False,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="crypto-data-project-dbt",
        command="dbt test",
        working_dir="/dbt",
        network_mode="crypto-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        mount_tmp_dir=False,
    )

    start >> sensor_on_staging_layer >> dbt_run >> dbt_test
