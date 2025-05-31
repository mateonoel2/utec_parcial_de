from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
import os
import pandas as pd
import sqlite3
import logging
log = logging.getLogger('airflow.task')
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"

BLOB_NAME = "raw/airflow/G30/matches/"
DB_PATH = "/opt/airflow/data/sports_league.sqlite"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id="dag_matches",
    description="Extrae matches de un día antes y los sube a Azure con fecha en el nombre.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * *",
    catchup=False,
    tags=["azure", "blob", "upload"],
)
def upload_dag():

    @task
    def extract_matches(ds=None):
        exec_date = datetime.strptime(ds, '%Y-%m-%d')
        day_before = exec_date - timedelta(days=1)
        day_before_str = day_before.strftime('%Y-%m-%d')

        conn = sqlite3.connect(DB_PATH)
        query = f"""
        SELECT * FROM matches
        WHERE utc_date = '{day_before_str}'
        """
        df = pd.read_sql(query, conn)
        conn.close()

        local_file_path = f"/opt/airflow/data/matches/{day_before_str}.parquet"
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        df.to_parquet(local_file_path, index=False)
        print(day_before_str)
        return local_file_path

    @task
    def call_upload(ds=None, local_file_path=None):
        exec_date = datetime.strptime(ds, '%Y-%m-%d')
        day_before = exec_date - timedelta(days=1)

        log.info(f"==>  {BLOB_NAME}")
        blob_name = add_date_suffix(BLOB_NAME, day_before)
        blob_name = blob_name + ".parquet"
        
        log.info(f"==>  {blob_name}")
        upload_to_adls(
            local_file_path=local_file_path,
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            wasb_conn_id=WASB_CONN_ID
        )
        print(f"Uploaded {local_file_path} to {CONTAINER_NAME}/{blob_name}")

    local_file_path = extract_matches()
    call_upload(local_file_path=local_file_path)

dag = upload_dag()
