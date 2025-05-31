from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from pendulum import timezone
from datetime import datetime, timedelta
from random import randint
import logging

# 1. Default arguments
default_args = {
    "start_date": datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 2. Define the DAG
@dag(
    dag_id="03_my_dag_pro",
    default_args=default_args,
    schedule=MultipleCronTriggerTimetable("@daily", timezone="America/Lima"),  # Airflow 3.0 style
    tags=["educational", "training", "branching"],
)
def my_training_pipeline():

    @task
    def training_model(model_name):
        accuracy = randint(1, 10)  # Random accuracy between 1 and 10
        logging.info(f"==> Training model {model_name} resulted in accuracy: {accuracy}")
        return accuracy
    
    @task.branch
    def choose_best_model(acc_a: int, acc_b: int, acc_c: int):
        # Choose the model with highest accuracy
        best_accuracy = max(acc_a, acc_b, acc_c)
        logging.info(f"==> Best model accuracy: {best_accuracy}")

        # Assume if best model > 5 it is accurate
        if best_accuracy > 5:
            return ["accurate", "bash_accurate"] # Match task_id below
        else:
            return ["inaccurate", "bash_inaccurate"]

    @task
    def accurate():
        print("==> Model is accurate!")

    @task
    def inaccurate():
        print("==> Model is inaccurate!")

    # BashOperators
    bash_accurate = BashOperator(
        task_id="bash_accurate",
        bash_command="echo '==> accurate'",
    )

    bash_inaccurate = BashOperator(
        task_id="bash_inaccurate",
        bash_command="echo '==> inaccurate'",
    )

    # 3. Define task execution
    acc_a = training_model("A")
    acc_b = training_model("B")
    acc_c = training_model("C")

    decision = choose_best_model(acc_a, acc_b, acc_c)
    
    # Branching endpoints
    accurate_task = accurate()
    inaccurate_task = inaccurate()

    # Branching: decision -> accurate path or inaccurate path
    decision >> [accurate_task, bash_accurate]
    decision >> [inaccurate_task, bash_inaccurate]

# Instantiate the DAG
dag = my_training_pipeline()
