from airflow.decorators import dag, task
from scripts.azure_upload import upload_to_adls
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="05_dag_azure_normal",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["azure", "blob", "upload"],
)
def upload_dag():

    @task
    def call_upload():
        upload_to_adls()

    call_upload()

dag = upload_dag()
