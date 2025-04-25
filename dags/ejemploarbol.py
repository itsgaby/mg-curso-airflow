from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejemplo_3_arbol',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    raiz = EmptyOperator(task_id='raiz')
    rama_izq = EmptyOperator(task_id='rama_izquierda')
    rama_der = EmptyOperator(task_id='rama_derecha')
    hoja_1 = EmptyOperator(task_id='hoja_1')
    hoja_2 = EmptyOperator(task_id='hoja_2')
    hoja_3 = EmptyOperator(task_id='hoja_3')

    raiz >> [rama_izq, rama_der]
    rama_izq >> [hoja_1, hoja_2]
    rama_der >> hoja_3