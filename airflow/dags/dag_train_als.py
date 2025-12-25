from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mlops",
    "retries": 0,
}

with DAG(
    dag_id="train_als_model",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # nightly
    catchup=False,
    default_args=default_args,
    tags=["training", "als"],
) as dag:

    train_als = BashOperator(
        task_id="train_als",
        bash_command="""
        cd /mnt/e/learn/movie_recommend_system_s/movie_recommend_system_3 &&
        spark-submit src/training/als_trainer.py
        """,
    )

    train_als

