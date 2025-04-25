#Una tarea inicial inicio.
#Una tarea intermedia proceso_principal.
#Desde ahí, dos caminos:
#Uno siempre se ejecuta: tarea_comun.
#Otro se ejecuta solo si es necesario: tarea_condicional.
#Ambas tareas deben finalizar antes de cierre.

#El nodo raíz se llama entrada.
#Desde entrada deben bifurcarse dos ramas: ruta_a y ruta_b.
#ruta_a se divide en dos tareas: sub_a1 y sub_a2.
#ruta_b se divide en tres tareas: sub_b1, sub_b2, y sub_b3.

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='ejercicio3',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    raiz = EmptyOperator(task_id='raiz')
    proceso_principal = EmptyOperator(task_id='proceso_principal')
    tarea_comun = EmptyOperator(task_id='tarea_comun')
    tarea_condicional = EmptyOperator(task_id='tarea_condicional')
    cierre = EmptyOperator(task_id='cierre')
   

    raiz >> proceso_principal
    proceso_principal >> [tarea_comun, tarea_condicional]
    [tarea_comun, tarea_condicional] >> cierre