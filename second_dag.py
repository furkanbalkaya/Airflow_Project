from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta    

def python_first_function(): pass   
default_dag_args = {
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}


with DAG("exercise_2", schedule_interval = '00 00 16 12 1', catchup=False, default_args = default_dag_args) as dag_python:   

    task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)

