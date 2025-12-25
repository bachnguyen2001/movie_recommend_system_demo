from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.training.popularity import generate_recommendations_for_all_users
from src.serving.write_recommendations import write_recommendations


def run_popularity():
    df = generate_recommendations_for_all_users(top_k=10)
    write_recommendations(df, model_version="popularity_global_v1")


with DAG(
    dag_id="popularity_recommender",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # chạy tay trước
    catchup=False,
    tags=["recommender", "baseline"],
) as dag:

    popularity_task = PythonOperator(
        task_id="run_popularity_model",
        python_callable=run_popularity,
    )
