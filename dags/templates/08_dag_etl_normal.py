from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

INPUT_FILE_PATH = "/opt/airflow/data/input.csv"
TRANSFORMED_FILE_PATH = "/opt/airflow/data/transformed.csv"
SUMMARY_FILE_PATH = "/opt/airflow/data/summary.csv"

def leer_datos(path):
    return pd.read_csv(path)

def transformar_datos(df):
    df['total'] = df['cantidad'] * df['precio']
    return df

def resumir_datos(df):
    resumen = df.groupby('producto')['total'].sum().reset_index()
    return resumen

def guardar_datos(df, path):
    df.to_csv(path, index=False)

@dag(
    dag_id='08_dag_etl_normal',
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["transformación", "ejemplo"]
)
def dag_etl_completo():
    
    @task
    def extraer():
        df = leer_datos(path=INPUT_FILE_PATH)
        return df.to_json()  # pasar como string serializable

    @task
    def transformar(df_json):
        df = pd.read_json(df_json)
        df_transformado = transformar_datos(df)
        guardar_datos(df_transformado, TRANSFORMED_FILE_PATH)
        return df_transformado.to_json()

    @task
    def resumir(df_json):
        df = pd.read_json(df_json)
        resumen = resumir_datos(df)
        guardar_datos(resumen, SUMMARY_FILE_PATH)
    
    datos = extraer()
    datos_transformados = transformar(datos)
    resumir(datos_transformados)

dag = dag_etl_completo()
