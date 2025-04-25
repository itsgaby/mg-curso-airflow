from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejemplo_1_lineal',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = EmptyOperator(task_id='inicio')
    t2 = EmptyOperator(task_id='paso_1')
    t3 = EmptyOperator(task_id='fin')

    t1 >> t2 >> t3