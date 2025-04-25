#El nodo raíz se llama entrada.
#Desde entrada deben bifurcarse dos ramas: ruta_a y ruta_b.
#ruta_a se divide en dos tareas: sub_a1 y sub_a2.
#ruta_b se divide en tres tareas: sub_b1, sub_b2, y sub_b3.

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejercicio2',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    raiz = EmptyOperator(task_id='raiz')
    ruta_a = EmptyOperator(task_id='ruta_a')
    ruta_b = EmptyOperator(task_id='ruta_b')
    sub_a1 = EmptyOperator(task_id='sub_a1')
    sub_a2 = EmptyOperator(task_id='sub_a2')
    sub_b1 = EmptyOperator(task_id='sub_b1')
    sub_b2 = EmptyOperator(task_id='sub_b2')
    sub_b3 = EmptyOperator(task_id='sub_b3')
   

    raiz >> [ruta_a, ruta_b]
    ruta_a >> [sub_a1, sub_a2]
    ruta_b >> [sub_b1,sub_b2,sub_b3]