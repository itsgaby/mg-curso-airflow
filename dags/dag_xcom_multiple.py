#Tenga una tarea generar_datos que envíe dos valores distintos usando xcom_push con las claves "mensaje" y "codigo".
#Tenga una tarea mostrar_datos que recupere ambos valores con xcom_pull e imprima un mensaje combinado.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push(**kwargs):
    kwargs['ti'].xcom_push(key='mensaje', value='Hola desde XCom')
    kwargs['ti'].xcom_push(key='codigo', value='con codigo')

def pull(**kwargs):
    valor1 = kwargs['ti'].xcom_pull(task_ids='push', key='mensaje')
    valor2 = kwargs['ti'].xcom_pull(task_ids='push', key='codigo')
    print(f"Mensaje recibido: {valor1}, Codigo recibido: {valor2}")

with DAG('dag_xcom_multiple', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='push', python_callable=push, provide_context=True)
    t2 = PythonOperator(task_id='pull', python_callable=pull, provide_context=True)

    t1 >> t2