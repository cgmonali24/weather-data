from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresOperator
import random
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Param
from weather_modules.utils import fetch_weather_data_from_db
from airflow.models import Variable
from dotenv import load_dotenv
import os
from weather_modules.fetch_data import fetch_weather_data
from weather_modules.insert_data import insert_weather_data


POSTGRES_CONNECTION_ID = Variable.get("postgres_conn_id")


with DAG(
    'weather_data_flow',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 2, 1),
        'retries': 1
    },
    schedule_interval='@daily',
    catchup=False,
    params={
        "start-date": Param("2025-02-04", type="string"),
        "end-date": Param("2025-02-05", type="string"),
    },
    render_template_as_native_obj=True
) as dag:

    fetch_weather_data_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True,
        op_kwargs=dag.params
    )

    create_table_task = PostgresOperator(
        task_id='create_weather_table',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data (
            datetime DATE ,
            temperature FLOAT,
            humidity INT
        );
        """
    )
    

    insert_data_task = PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data,
        provide_context=True,
        op_kwargs={
            'weather_data': "{{ task_instance.xcom_pull(task_ids='fetch_weather_data')}}",
            'postgres_conn_id': POSTGRES_CONNECTION_ID
        }
    )
    

    fetch_from_db_task = PythonOperator(
        task_id='fetch_weather_data_from_db',
        python_callable=fetch_weather_data_from_db,
        op_args=["SELECT * FROM weather_data", '/opt/airflow/dags/tmp/weather_data.csv', POSTGRES_CONNECTION_ID]
    )

    fetch_weather_data_task >> create_table_task >> insert_data_task >> fetch_from_db_task 

