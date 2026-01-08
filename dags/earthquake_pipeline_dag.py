from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from earthquake_elt.pipeline import EarthquakePipeline


def run_pipeline():
    pipeline = EarthquakePipeline()
    pipeline.run()


with DAG(
    dag_id="earthquake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["earthquake", "etl"],
) as dag:

    run_task = PythonOperator(
        task_id="run_earthquake_pipeline",
        python_callable=run_pipeline,
    )
