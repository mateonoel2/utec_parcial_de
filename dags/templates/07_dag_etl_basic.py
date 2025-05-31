from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

INPUT_FILE_PATH = "/opt/airflow/data/input.csv"
OUTPUT_FILE_PATH = "/opt/airflow/data/output.csv"

def transformar_datos(input_file_path, output_file_path):
    df = pd.read_csv(input_file_path)
    df['total'] = df['cantidad'] * df['precio']
    df.to_csv(output_file_path, index=False)

@dag(
    dag_id='07_dag_etl_basic',
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["transformación", "ejemplo"]
)
def dag_transformacion():
    
    @task
    def task_transform():
        transformar_datos(input_file_path=INPUT_FILE_PATH, output_file_path=OUTPUT_FILE_PATH)

    task_transform()
dag = dag_transformacion()