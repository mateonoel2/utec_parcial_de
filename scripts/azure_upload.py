from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
import os

log = logging.getLogger('airflow.task')

def delete_blob_from_adls(container_name, blob_name, wasb_conn_id="utec_blob_storage"):
    hook = WasbHook(wasb_conn_id=wasb_conn_id)

    if hook.check_for_blob(container_name=container_name, blob_name=blob_name):
        hook.delete_file(container_name=container_name, blob_name=blob_name)
        print(f"Archivo eliminado: {blob_name}")
    else:
        print(f"Archivo no encontrado: {blob_name}")

def upload_to_adls(
        local_file_path = "/opt/airflow/data/sample.txt",
        container_name = "airflow",
        blob_name = "raw/uploaded_sample.txt",
        wasb_conn_id = "utec_blob_storage"
        ):
    try:
        if not os.path.exists(local_file_path):
            log.warning(f"==> Local file not found: {local_file_path}")
            return
        
        # Connect using the connection created in UI
        hook = WasbHook(wasb_conn_id=wasb_conn_id)    
        hook.load_file(
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        log.info(f"==> Uploaded {local_file_path} to {container_name}/{blob_name}")
    except Exception as e:
        log.error("==> Failed to upload to Azure Blob", exc_info=True)
        raise
