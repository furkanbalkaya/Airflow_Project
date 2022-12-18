import requests 
import time 
import json 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta 
import pandas as pd 
import numpy as np 
import os

def get_data(**kwargs): 
    api_key = 'IQZVJAU9X1PSX9YW'
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&apikey='+api_key
    r = requests.get(url)
    data = r.json()

print(data)

default_dag_args = {
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

with DAG("exercise_3", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:

    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'tickers' : []})
