from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta

LOCAL_FILE_PATH = "/opt/airflow/data/transacciones_bancarias.txt"
CONTAINER_NAME = "airflow"
BLOB_NAME = "raw/G15/uploaded_sample.txt"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="06_dag_azure_pro",
    description="Uploads a local file to Azure Blob Storage with a date suffix.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=["azure", "blob", "upload"],
)
def upload_dag():

    @task
    def call_upload():
        new_blob_name = add_date_suffix(BLOB_NAME)
        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name
            )

    call_upload()

dag = upload_dag()
