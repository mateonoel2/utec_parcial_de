from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
# from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from random import randint
from datetime import datetime

def _choose_best_model(ti):
    # When using BranchPythonOperator, we need to give the task_id of the next task we want to execute
    # ti: Task instance object
    accuracies = ti.xcom_pull(task_ids=[
        "training_model_A",
        "training_model_B",
        "training_model_C"
    ])
    best_accuracy = max(accuracies)

    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

def _training_model():
    return randint(1, 10)

with DAG(
    dag_id="01_my_dag_basic",
    start_date=datetime(2025,1,1),
    # timetable=CronDataIntervalTimetable(cron="@daily", timezone="UTC"),
    schedule=MultipleCronTriggerTimetable("@daily", timezone="UTC"),
    catchup=False
    ) as dag:
    # schedule_interval="@daily"    : Cron expression or pre-sets cron expressions. See https://crontab.guru/
    # catchup=False                 : This way only latest no triggered diagram will be automatically triggered
    # dag                           : each time a dag is triggered a diagram object is created,
    #                                 which is an instance of the dag running at a given day
    # XCOMS                         : (Cross Communication Messages) Mecanism to share data between tasks in our dag

    task_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )

    task_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model
    )

    task_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_task = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    task_accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    task_inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    [task_A, task_B, task_C ]>> choose_best_task >> [task_accurate, task_inaccurate]