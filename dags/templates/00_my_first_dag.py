from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, Airflow!")

with DAG(
    dag_id="00_my_first_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  # This runs the DAG once per day
    catchup=False  # Don't run past scheduled runs
) as dag:
    
    hello_task = PythonOperator(
        task_id="print_hello_task",  # Task name in Airflow
        python_callable=print_hello  # Function to run when the task is triggered
    )

    hello_task  # Since it's the only task, no need to chain tasks
