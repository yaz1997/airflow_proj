from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.filesystem import FileSensor  
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Solo project on DAG

default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 9, 19),
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }


dag = DAG(
    'DAG_project',
    default_args=default_args,
    schedule_interval = timedelta(days=1),  
    catchup = False, 
    start_date = default_args['start_date'],  
)


api_check_task = HttpSensor(
    task_id='check_api_availability',
    method = 'GET',
    http_conn_id='http_local',  
    endpoint='',  
    timeout=20,  
    mode='reschedule', 
    poke_interval=60,  
    dag=dag,
)




def extract_data_and_save_to_csv():
   
    api_url = 'https://bored-api.appbrewery.com/filter?type=education'  

    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            # Parse the JSON response
            api_data = response.json()

            df = pd.DataFrame(api_data)

            local_csv_path = '/home/riyaz/airflow/proj_data/api_data.csv'  

            df.to_csv(local_csv_path, index=False)

            return f"Data saved to {local_csv_path}"
        else:
            return f"API request failed with status code {response.status_code}"

    except Exception as e:
        return f"Error: {str(e)}"



extract_and_save_task = PythonOperator(
    task_id='extract_and_save_data_task',
    python_callable=extract_data_and_save_to_csv,
    dag=dag,
)

# defining connection ID and table name for PG
postgres_conn_id = 'postgres_local'

postgres_table_name = 'api_to_pg'


file_sensor = FileSensor(
    task_id = 'wait_for_csv_file',
    poke_interval = 30,  
    timeout = 3600,  
    fs_conn_id = 'fs_default',  
    filepath = '/home/riyaz/airflow/proj_data/api_data.csv',  
    mode = 'poke',
    dag = dag,
)

move_file_task = BashOperator(
    task_id='move_file_to_tmp',
    bash_command='mv /home/riyaz/airflow/proj_data/api_data.csv /tmp/csv/',  
)

file_sensor1 = FileSensor(
    task_id = 'wait_for_csv_file_move',
    poke_interval = 30,  
    timeout = 3600,  
    fs_conn_id = 'fs_default',  
    filepath = '/tmp/csv/api_data.csv',  
    mode = 'poke',
    dag = dag,
)


# Define a PostgresOperator to load data into PostgreSQL
load_data_to_postgres = PostgresOperator(
    task_id = 'load_data_to_postgres',
    sql="""
        COPY api_to_pg FROM '/tmp/csv/api_data.csv' CSV HEADER;
        """,
    postgres_conn_id = postgres_conn_id,  
    dag = dag,
)


spark_submit = BashOperator(
        task_id='spark_submit_task',
        bash_command="spark-submit /home/riyaz/airflow/dags/spark_sub.py",
    )


read_table = PostgresOperator(
        sql = "select * from question_1",
        task_id = "read_table_task",
        postgres_conn_id = postgres_conn_id,
        autocommit=True
    )

def process_postgres_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='read_table_task')
        # Process the result data here
        for row in result:
            print(row)

process_data_task = PythonOperator(
        task_id='process_postgres_result',
        python_callable=process_postgres_result,
        provide_context=True
    )


api_check_task >> extract_and_save_task >> file_sensor >> move_file_task >> file_sensor1 >> load_data_to_postgres >> spark_submit >>  read_table >> process_data_task
