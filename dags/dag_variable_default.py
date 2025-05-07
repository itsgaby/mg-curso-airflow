#Tenga una tarea generar_datos que envíe dos valores distintos usando xcom_push con las claves "mensaje" y "codigo".
#Tenga una tarea mostrar_datos que recupere ambos valores con xcom_pull e imprima un mensaje combinado.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

def leervariable(**kwargs):
    entorno = Variable.get('entorno', default_var='desarrollo')
    print(f"Variable: {entorno}")

with DAG('dag_xcom_default', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    imprimir_entorno = PythonOperator(task_id='leervariable', python_callable=leervariable, provide_context=True)

    imprimir_entorno