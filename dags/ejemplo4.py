from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejemplo_4_etapas',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    inicio = EmptyOperator(task_id='inicio')
    etapa_1 = EmptyOperator(task_id='etapa_1')
    etapa_2 = EmptyOperator(task_id='etapa_2')
    etapa_3 = EmptyOperator(task_id='etapa_3')
    union = EmptyOperator(task_id='union')
    fin = EmptyOperator(task_id='fin')

    inicio >> [etapa_1, etapa_2, etapa_3] >> union >> fin