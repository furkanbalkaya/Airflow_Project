from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta    


default_dag_args = {
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}


with DAG("exercise_1", schedule_interval= None, default_args =   default_dag_args) as dag:


    task_0 = BashOperator(task_id = 'bash_task', bash_command = "echo 'command executed from Bash Operater' ")
    task_1 = BashOperator(task_id = 'bash_task_move_data', bash_command = "cp  C:/Users/furka/data_center/data_lake/dataset_raw.txt.txt   C:/Users/furka/data_center/clean_data")
    task_2 = BashOperator(task_id = 'bash_task_remove_data', bash_command = "rm C:/Users/furka/data_center/data_lake/dataset_raw.txt")

    task_0 >> task_1 >> task_2                               
