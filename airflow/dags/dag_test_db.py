from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from common.db import get_pg_engine
import pandas as pd


def test_db_connection():
    engine = get_pg_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT COUNT(*) AS cnt FROM ratings", engine)
    print(df)


with DAG(
    dag_id="test_db_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "db"],
) as dag:

    test_db = PythonOperator(
        task_id="test_db",
        python_callable=test_db_connection,
    )
