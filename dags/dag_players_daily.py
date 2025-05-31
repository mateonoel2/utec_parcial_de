from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
import great_expectations as gx


LOCAL_FILE_PATH = "/opt/airflow/data/players.csv"
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G30/players/"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="dag_players_daily",
    description="Extrae los jugadores de manera continua cuando hay partidos",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 8 * * *", # Runs every day at 8:00 local time (GMT-5)
    catchup=False,
    tags=["utec", "players", "daily"],
)
def upload_dag():

    @task
    def call_upload():
        new_blob_name = add_date_suffix(BLOB_NAME)
        new_blob_name = new_blob_name + ".parquet"
        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id = WASB_CONN_ID
            )

    call_upload()

dag = upload_dag()
