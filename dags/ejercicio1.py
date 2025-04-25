from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

#Una tarea inicial llamada inicio.
#Tres tareas paralelas que se ejecutan después de inicio: verificacion_a, verificacion_b y verificacion_c.
#Todas deben completarse para poder ejecutar la tarea final conclusion.

with DAG(
    dag_id='ejercicio1',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    inicio = EmptyOperator(task_id='inicio')
    tarea_1 = EmptyOperator(task_id='verificacion_a')
    tarea_2 = EmptyOperator(task_id='verificacion_b')
    tarea_3 = EmptyOperator(task_id='verificacion_c')
    fin = EmptyOperator(task_id='fin')

    inicio >> [tarea_1, tarea_2, tarea_3] >> fin