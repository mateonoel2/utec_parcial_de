from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from pendulum import timezone
from datetime import datetime, timedelta
from random import randint

# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator

# 1. Default arguments
default_args = {
    "start_date": datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 2. Define the DAG
@dag(
    dag_id="02_my_dag_normal",
    default_args=default_args,
    schedule=MultipleCronTriggerTimetable("@daily", timezone="UTC"),  # Airflow 3.0 style
    tags=["example", "training"],
)
def my_training_pipeline():

    @task
    def training_model(model_name):
        accuracy = randint(1, 10)  # Random accuracy between 1 and 10
        print(f"Training model {model_name} resulted in accuracy: {accuracy}")
        return accuracy
    
    @task.branch
    def choose_best_model(acc_a: int, acc_b: int, acc_c: int):
        # Choose the model with highest accuracy
        best_accuracy = max(acc_a, acc_b, acc_c)
        print(f"Best model accuracy: {best_accuracy}")

        # Assume if best model > 5 it is accurate
        if best_accuracy > 8:
            return "accurate"
        else:
            return "inaccurate"

    @task
    def accurate():
        print("Model is accurate!")

    @task
    def inaccurate():
        print("Model is inaccurate!")

    # 3. Define task execution
    acc_a = training_model("A")
    acc_b = training_model("B")
    acc_c = training_model("C")

    decision = choose_best_model(acc_a, acc_b, acc_c)

    # Branching endpoints
    accurate_task = accurate()
    inaccurate_task = inaccurate()

    decision >> [accurate_task, inaccurate_task]

# Instantiate the DAG
dag = my_training_pipeline()
