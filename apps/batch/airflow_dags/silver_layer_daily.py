from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DEFAULT_ARGS = {
    "owner": "recgym",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="silver_layer_daily",
    description="Bronze → Silver (events, users, items) daily pipeline.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["recgym", "batch", "silver"],
) as dag:
    silver_events = SparkSubmitOperator(
        task_id="silver_events",
        application="/opt/recgym/apps/batch/silver/events.py",
        name="silver-events",
        conn_id="spark_default",
        verbose=True,
        application_args=[],
        conf={
            "spark.master": "spark://spark:7077",
            "spark.submit.deployMode": "client",
        },
    )

    silver_users = SparkSubmitOperator(
        task_id="silver_users",
        application="/opt/recgym/apps/batch/silver/users.py",
        name="silver-users",
        conn_id="spark_default",
        verbose=True,
        application_args=[],
        conf={
            "spark.master": "spark://spark:7077",
            "spark.submit.deployMode": "client",
        },
    )

    silver_items = SparkSubmitOperator(
        task_id="silver_items",
        application="/opt/recgym/apps/batch/silver/items.py",
        name="silver-items",
        conn_id="spark_default",
        verbose=True,
        application_args=[],
        conf={
            "spark.master": "spark://spark:7077",
            "spark.submit.deployMode": "client",
        },
    )

    silver_events >> [silver_users, silver_items]
