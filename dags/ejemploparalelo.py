from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejemplo_2_paralelo',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    inicio = EmptyOperator(task_id='inicio')
    tarea_1 = EmptyOperator(task_id='tarea_1')
    tarea_2 = EmptyOperator(task_id='tarea_2')
    fin = EmptyOperator(task_id='fin')

    inicio >> [tarea_1, tarea_2] >> fin