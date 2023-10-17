# interaction with postgres database in airflow

from airflow import DAG
import time
import json
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

default_dag_args={
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

create_query="""
DROP TABLE IF EXISTS public.employee;
CREATE TABLE public.employee (name VARCHAR(250), age SMALLINT);
"""

insert_data_query = """ 
INSERT INTO public.employee (name, age)
VALUES ('Alice', 20), ('Bob', 30), ('Charlie', 25) 
"""

calculating_average_age = """
DROP TABLE IF EXISTS public.employee_average_age;
CREATE TABLE IF NOT EXISTS public.employee_average_age AS
SELECT ROUND(AVG(age), 2)
FROM public.employee 
"""




dag_postgres= DAG(dag_id="postgres_dag_connect_ex",default_args=default_dag_args, schedule_interval=None, start_date=days_ago(1))

create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_gian_local")

insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_gian_local")

group_data = PostgresOperator(task_id = "calculating_average_age", sql = calculating_average_age, dag = dag_postgres, postgres_conn_id = "postgres_gian_local")

create_table >> insert_data >> group_data