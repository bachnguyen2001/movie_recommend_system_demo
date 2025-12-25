from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="recommender_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    load_users = BashOperator(
        task_id="load_users",
        bash_command="python -m src.ingestion.load_users {{ ds }}",
        env={
            "AIRFLOW_CONN_POSTGRES_RECOMMENDER": "{{ conn.postgres_recommender.get_uri() }}"
        },
    )
