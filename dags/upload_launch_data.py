import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def choose_destination(**kwargs):
    return kwargs['params'].get('output_target', 'to_ftp')

def upload_to_ftp():
    hook = FTPHook(ftp_conn_id='ftp_default')
    hook.store_file('/launches.json', '/tmp/launches.json')

def upload_to_db():
    with open('/tmp/launches.json') as f:
        data = json.load(f)
    pg = PostgresHook(postgres_conn_id='postgres_local')
    for launch in data['results']:
        pg.run("INSERT INTO launches (id, name, net) VALUES (%s, %s, %s)",
               parameters=(launch['id'], launch['name'], launch['net']))

default_args = {'start_date': datetime(2025, 5, 1)}

with DAG('upload_launch_data',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    download = BashOperator(
        task_id='download_json',
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'"
    )

    choose = BranchPythonOperator(
        task_id='choose_target',
        python_callable=choose_destination,
        provide_context=True
    )

    to_ftp = PythonOperator(
        task_id='to_ftp',
        python_callable=upload_to_ftp
    )

    to_db = PythonOperator(
        task_id='to_db',
        python_callable=upload_to_db
    )

    end = BashOperator(task_id='done', bash_command='echo \"Data uploaded.\"')

    download >> choose >> [to_ftp, to_db] >> end