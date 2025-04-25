from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejemplo_5_pipeline_complejo',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    inicio = EmptyOperator(task_id='inicio')
    extraccion = EmptyOperator(task_id='extraccion')

    transformacion_1 = EmptyOperator(task_id='transformacion_1')
    transformacion_2 = EmptyOperator(task_id='transformacion_2')
    transformacion_3 = EmptyOperator(task_id='transformacion_3')

    union_transformaciones = EmptyOperator(task_id='union_transformaciones')
    carga = EmptyOperator(task_id='carga')
    fin = EmptyOperator(task_id='fin')

    inicio >> extraccion
    extraccion >> [transformacion_1, transformacion_2, transformacion_3]
    [transformacion_1, transformacion_2, transformacion_3] >> union_transformaciones
    union_transformaciones >> carga >> fin