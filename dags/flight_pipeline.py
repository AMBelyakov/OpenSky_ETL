import json
import os
import sys
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago

sys.path.append("/opt/airflow/scripts")

from extract.opensky import BBOX, poll_states
from extract.stations import generate_stations

DATA_DIR = "/opt/synthetic_data/"
BUCKET_NAME = "etl-bucket"

default_args = {
    "owner": "AMBelyakov",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="flight_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    description="Incremental ADS-B flight tracking pipeline (OpenSky) with DQ checks",
    tags=["adsb", "opensky", "s3", "dwh", "spark"],
)


def get_bbox():
    """
    Зона наблюдения из Variable, с падением на дефолт из opensky.py.

    Вынесена в Variable, потому что плотность бортов у OpenSky определяется
    покрытием волонтёрских приёмников и меняется от региона к региону на
    порядки.
    """
    raw = Variable.get("OPENSKY_BBOX", default_var=None)
    return json.loads(raw) if raw else BBOX


def extract_stations():
    """
    Станции фильтруем той же зоной, что и борта.
    """
    return generate_stations(output_dir=DATA_DIR, bbox=get_bbox())


def extract_states():
    """
    Собирает треки бортов серией снапшотов OpenSky.
    """
    window_sec = int(Variable.get("OPENSKY_WINDOW_SEC", default_var=1800))
    interval_sec = int(Variable.get("OPENSKY_INTERVAL_SEC", default_var=20))

    return poll_states(
        output_dir=DATA_DIR,
        bbox=get_bbox(),
        window_sec=window_sec,
        interval_sec=interval_sec,
    )


def convert_to_parquet(**context):
    ds = context["ds"]

    files = ["states", "stations"]
    output_dir = os.path.join(DATA_DIR, f"parquet/date={ds}/")
    os.makedirs(output_dir, exist_ok=True)

    for name in files:
        csv_path = os.path.join(DATA_DIR, f"{name}.csv")
        pq_path = os.path.join(output_dir, f"{name}.parquet")
        df = pd.read_csv(csv_path)
        df.to_parquet(pq_path, index=False)
        print(f"Converted {name}.csv → {name}.parquet ({len(df)} rows)")


def upload_to_s3(**context):
    ds = context["ds"]
    local_parquet_dir = os.path.join(DATA_DIR, f"curated/date={ds}/flight_points")
    s3_prefix = f"flight_points/date={ds}"

    if not os.path.exists(local_parquet_dir):
        raise FileNotFoundError(
            f"Папка {local_parquet_dir} не найдена! Spark возможно не отработал."
        )

    hook = S3Hook(aws_conn_id="minio_s3_conn")

    files_uploaded = 0
    for root, _, files in os.walk(local_parquet_dir):
        for file_name in files:
            # Служебные файлы Spark (_SUCCESS, контрольные суммы) в S3 не нужны.
            if file_name.startswith("_") or file_name.endswith(".crc"):
                continue

            local_file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(root, local_parquet_dir)
            s3_key = (
                f"{s3_prefix}/{file_name}"
                if relative_path == "."
                else f"{s3_prefix}/{relative_path}/{file_name}"
            )

            hook.load_file(
                filename=local_file_path,
                key=s3_key,
                bucket_name=BUCKET_NAME,
                replace=True,
            )
            files_uploaded += 1

    if files_uploaded == 0:
        raise ValueError(
            f"В папке {local_parquet_dir} не найдено Parquet-файлов для загрузки!"
        )

    print(f"✅ Успешно загружено {files_uploaded} файлов в S3 за дату {ds}")


stations_task = PythonOperator(
    task_id="extract_stations",
    python_callable=extract_stations,
    dag=dag,
)

states_task = PythonOperator(
    task_id="extract_states",
    python_callable=extract_states,
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

convert_task = PythonOperator(
    task_id="convert_to_parquet",
    python_callable=convert_to_parquet,
    dag=dag,
)

transform_task = SparkSubmitOperator(
    task_id="spark_transform_data",
    application="/opt/airflow/scripts/transform/flight_transform.py",
    conn_id="spark_default",
    conf={
        "spark.master": "local[*]",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
    },
    jars="/opt/spark/jars/postgresql-42.7.5.jar,/opt/spark/jars/clickhouse-jdbc-0.9.7-all.jar",
    application_args=["--date", "{{ ds }}"],
    dag=dag,
)

s3_upload_task = PythonOperator(
    task_id="upload_curated_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

send_success_telegram = TelegramOperator(
    task_id="send_success_telegram",
    telegram_conn_id="artemw9_tg",
    chat_id="{{ var.value.Artemw9_TELEGRAM_CHAT_ID }}",
    text=(
        "✅ <b>Flight Pipeline</b> успешно завершен!\n"
        "📅 Дата: <code>{{ ds }}</code>\n"
        "🛫 Треки OpenSky загружены в ClickHouse и S3."
    ),
    dag=dag,
)

send_failure_telegram = TelegramOperator(
    task_id="send_failure_telegram",
    telegram_conn_id="artemw9_tg",
    chat_id="{{ var.value.Artemw9_TELEGRAM_CHAT_ID }}",
    text=(
        "❌ <b>Flight Pipeline УПАЛ!</b>\n"
        "📅 Дата: <code>{{ ds }}</code>\n"
        "🚨 Ошибка в задаче: <code>{{ task_instance.task_id }}</code>\n"
        "📝 Логи: {{ task_instance.log_url }}"
    ),
    trigger_rule="one_failed",
    dag=dag,
)

(
    [stations_task, states_task]
    >> convert_task
    >> transform_task
    >> s3_upload_task
    >> [send_success_telegram, send_failure_telegram]
)
