from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def saludo(**kwargs):
    nombre = Variable.get('nombre_usuario')
    print(f"Bienvenido, {nombre}")

with DAG('dag_ejercicio_variables', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    saludo_task = PythonOperator(task_id='saludo', python_callable=saludo, provide_context=True)