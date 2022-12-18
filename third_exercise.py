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
    ticker = kwargs['ticker']
    api_key = 'IQZVJAU9X1PSX9YW'
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol='+ticker+'&apikey='+api_key
    r = requests.get(url)

    try: 
        data = r.json()
        path  = "C:/Users/furka/data_center/data_lake"
        with open(path+ "stock_market_raw_data_ "+ticker + '_'+ str(time.time()), "w") as outfile:
            json.dump(data, outfile)
    except: pass
    

def clean_data(**kwargs): 
    read_path  = "C:/Users/furka/data_center/data_lake"
    
    ticker = kwargs['ticker']
    latest = np.max([float(file.split("_")[-1]) for file in os.listdir(read_path) if ticker in file ])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    output_path = "C:/Users/furka/data_center/clean_data"

    file = open(read_path + latest_file)
    data = json.load(file)

    clean_data = pd.DataFrame(data['Time Series (Weekly)']).T
    clean_data['ticker'] = data["Meta Data"]['2. Symbol']
    clean_data['meta_data'] = str(data['Meta Data'])
    clean_data['timestamp'] = pd.to_datetime('now')

    clean_data.to_csv(output_path + ticker +"_Weekly" + str(pd.to_datetime('now'))+'.csv')

default_dag_args = {
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

with DAG("exercise_3", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:

    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'ticker' : "IBM"})
    task_1 = PythonOperator(task_id = "clean_market_data", python_callable = clean_data, op_kwargs = {'ticker' : "IBM"})

    task_0 >> task_1

