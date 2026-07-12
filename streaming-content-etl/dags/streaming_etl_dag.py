"""
Airflow DAG: обработка ОДНОЙ бизнес-даты стримингового сервиса.

    wait_for_raw_partition -> transform (PySpark) -> data_quality -> load (ClickHouse)

Каждый запуск обрабатывает свою дату (`{{ ds }}`): ждёт партицию сырых данных,
считает витрины только за неё и идемпотентно перезаливает партицию в ClickHouse.
Исторический период заливается backfill'ом:

    airflow dags backfill -s 2024-09-01 -e 2024-12-11 streaming_etl_pipeline
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

ETL_HOME = os.environ.get("ETL_HOME", "/opt/airflow/project")
PY = os.environ.get("ETL_PYTHON", "python")

RAW = f"{ETL_HOME}/data/raw"
MARTS = f"{ETL_HOME}/data/marts"

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _wait_for_raw_partition(ds: str, **_) -> None:
    """Аналог S3KeySensor: партиция сырых данных за дату должна существовать."""
    path = f"{RAW}/audition/business_dt={ds}"
    if not os.path.isdir(path):
        raise FileNotFoundError(f"нет партиции сырых данных за {ds}: {path}")
    print(f"партиция за {ds} на месте")


with DAG(
    dag_id="streaming_etl_pipeline",
    description="Инкрементальный ETL по бизнес-дате: PySpark -> ClickHouse",
    schedule="@daily",
    start_date=datetime(2024, 9, 1),
    end_date=datetime(2024, 12, 11),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "pyspark", "clickhouse", "incremental"],
) as dag:

    wait_for_raw_partition = PythonOperator(
        task_id="wait_for_raw_partition",
        python_callable=_wait_for_raw_partition,
    )

    transform = BashOperator(
        task_id="transform_pyspark",
        bash_command=f"{PY} {ETL_HOME}/src/transform.py --raw {RAW} --out {MARTS} --date {{{{ ds }}}}",
    )

    data_quality = BashOperator(
        task_id="data_quality",
        bash_command=f"{PY} {ETL_HOME}/src/data_quality.py --marts {MARTS} --date {{{{ ds }}}}",
    )

    load = BashOperator(
        task_id="load_clickhouse",
        bash_command=f"{PY} {ETL_HOME}/src/load_clickhouse.py --marts {MARTS} --date {{{{ ds }}}}",
    )

    wait_for_raw_partition >> transform >> data_quality >> load
