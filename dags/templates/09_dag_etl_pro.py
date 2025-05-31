from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from scripts.helpers import leer_datos, transformar_datos, resumir_datos, guardar_datos
import logging

INPUT_FILE_PATH = "/opt/airflow/data/input.csv"
TRANSFORMED_FILE_PATH = "/opt/airflow/data/transformed.csv"
SUMMARY_FILE_PATH = "/opt/airflow/data/summary.csv"

@dag(
    dag_id='09_dag_etl_pro',
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["transformación", "ejemplo"]
)
def dag_etl_completo():
    
    @task
    def extraer():
        logging.info(f"==> Extrayendo datos")
        df = leer_datos(path=INPUT_FILE_PATH)
        return df.to_json()  # pasar como string serializable

    @task
    def transformar(df_json):
        logging.info(f"==> Transformando datos")
        df = pd.read_json(df_json)
        df_transformado = transformar_datos(df)
        guardar_datos(df_transformado, TRANSFORMED_FILE_PATH)
        return df_transformado.to_json()

    @task
    def resumir(df_json):
        logging.info(f"==> Resumiendo datos")
        df = pd.read_json(df_json)
        resumen = resumir_datos(df)
        guardar_datos(resumen, SUMMARY_FILE_PATH)
    
    datos = extraer()
    datos_transformados = transformar(datos)
    resumir(datos_transformados)

dag = dag_etl_completo()
