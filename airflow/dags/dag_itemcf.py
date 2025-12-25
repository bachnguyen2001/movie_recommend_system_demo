from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.serving.itemcf_inference import generate_itemcf_recommendations
from src.serving.write_recommendations import write_recommendations


def run_itemcf():
    df = generate_itemcf_recommendations()
    write_recommendations(df, model_version="itemcf_v1")


with DAG(
    dag_id="itemcf_recommender",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # chạy tay trước
    catchup=False,
    tags=["recommender", "itemcf"],
) as dag:

    run_itemcf_task = PythonOperator(
        task_id="run_itemcf",
        python_callable=run_itemcf,
    )
