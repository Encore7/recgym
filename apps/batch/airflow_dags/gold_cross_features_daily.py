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
    dag_id="gold_cross_features_daily",
    description="Gold user + Gold item + Silver events → Gold cross features.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["recgym", "batch", "gold", "cross"],
) as dag:
    gold_cross_features = SparkSubmitOperator(
        task_id="gold_cross_features",
        application="/opt/recgym/apps/batch/gold/cross_features.py",
        name="gold-cross-features",
        conn_id="spark_default",
        verbose=True,
        application_args=[],
        conf={
            "spark.master": "spark://spark:7077",
            "spark.submit.deployMode": "client",
        },
    )
