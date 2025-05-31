from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="04_dag_azure_basic",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["azure", "blob", "upload"],
)
def upload_dag():

    @task
    def upload_file_to_blob():
        # Connect using the connection created in UI
        hook = WasbHook(wasb_conn_id="azure_blob_storage")
        
        # Local file path (this must exist in your container or volume)
        local_file_path = "/opt/airflow/data/sample.txt"
        
        # Container name in your blob storage
        container_name = "airflow"
        
        # The name the blob will have in Azure
        blob_name = "raw/uploaded_sample_v1.txt"

        # Upload the file
        hook.load_file(
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        print(f"Uploaded {local_file_path} to {container_name}/{blob_name}")

    upload_file_to_blob()

dag = upload_dag()
