from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mlops",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_ratings_to_parquet",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "ratings"],
) as dag:

    ingest = BashOperator(
        task_id="export_ratings",
        bash_command="""
        cd /mnt/e/learn/movie_recommend_system_s/movie_recommend_system_3 &&
        uv run python src/ingestion/export_ratings_to_parquet.py
        """,
    )

    ingest

