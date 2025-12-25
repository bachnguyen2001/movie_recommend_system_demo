from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mlops",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="generate_recommendations",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["serving", "recommendation"],
) as dag:

    generate_als = BashOperator(
        task_id="generate_als_recommendations",
        bash_command="uv run python src/jobs/generate_als_recommendations.py",
    )

    generate_itemcf = BashOperator(
        task_id="generate_itemcf_recommendations",
        bash_command="uv run python src/jobs/generate_itemcf_recommendations.py",
    )

    generate_popularity = BashOperator(
        task_id="generate_popularity_recommendations",
        bash_command="uv run python src/jobs/generate_popularity_recommendations.py",
    )

    generate_als >> generate_itemcf >> generate_popularity
