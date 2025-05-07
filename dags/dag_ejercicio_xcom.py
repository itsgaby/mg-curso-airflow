from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push(**kwargs):
    kwargs['ti'].xcom_push(key='mensaje', value='Hola desde XCom')

def pull(**kwargs):
    valor = kwargs['ti'].xcom_pull(task_ids='push', key='mensaje')
    print(f"Mensaje recibido: {valor}")

with DAG('dag_ejercicio_xcom', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='push', python_callable=push, provide_context=True)
    t2 = PythonOperator(task_id='pull', python_callable=pull, provide_context=True)

    t1 >> t2