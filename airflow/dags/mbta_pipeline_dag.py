from airflow.sdk import dag, task
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "yassir",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="mbta_ingestion_pipeline",
    description="Fetch MBTA vehicle/prediction data and load into the warehouse",
    schedule="*/2 * * * *",   # every 2 minutes
    start_date=datetime(2026, 9, 18),
    catchup=False,
    default_args=default_args,
    tags=["mbta"],
)
def mbta_ingestion_pipeline():

    @task
    def fetch_data():
        subprocess.run(
            ["python", "/opt/airflow/project/ingestion/fetch_once.py"],
            check=True
        )

    @task
    def transform_data():
        subprocess.run(
            ["python", "/opt/airflow/project/transform/transform_facts.py"],
            check=True
        )

    fetch_data() >> transform_data()


mbta_ingestion_pipeline()