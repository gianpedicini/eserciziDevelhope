import requests
import time
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import os


#exercise: write a DAG which is able to request market data for a list of stocks.
api_key="GJBUU15OX4LHDW43"

def get_data(**kwargs):
    ticker=kwargs["ticker"]

    print("opened")

    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol='+ ticker +'&apikey='+api_key
    r = requests.get(url)
    
    data = r.json()
    path= "/Users/gian/Desktop/Develhope/DATA_CENTER/DATA_LAKE/"
    with open(path+ "stock_market_raw_data_ex_" + ticker + "_" + str(time.time()),"w") as outfile:
        json.dump(data,outfile)

default_dag_args={
    "start_date": datetime(2023,9,20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries":1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:

    task_0=PythonOperator(task_id="get_data", python_callable=get_data, op_kwargs={"ticker":"IBM"})
