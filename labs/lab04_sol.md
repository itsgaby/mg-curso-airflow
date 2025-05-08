## **Solución Ejercicio 1: Descargar datos de otra entidad de SpaceDevs API**

```python
import requests
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

def descargar_entidad():
    # Puedes cambiar la entidad a 'mission', 'agency', etc.
    entidad = 'astronaut'
    url = f'https://ll.thespacedevs.com/2.0.0/{entidad}/'

    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(f'/tmp/{entidad}.json', 'w') as f:
                f.write(response.text)
            print(f"Datos de la entidad '{entidad}' guardados en /tmp/{entidad}.json")
        else:
            raise Exception(f"Error en la solicitud: {response.status_code}")
    except Exception as e:
        print(f"Fallo al descargar datos de la entidad {entidad}: {str(e)}")

with DAG(
    dag_id='download_entity_data',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    catchup=False,
    description="Descarga los datos de una entidad de la SpaceDevs API y los guarda en disco.",
) as dag:

    tarea_descarga = PythonOperator(
        task_id='descargar_datos_entidad',
        python_callable=descargar_entidad,
    )
```

---

###  Personalización

Si deseas cambiar la entidad, simplemente modifica esta línea en la función:

```python
entidad = 'astronaut'  # por ejemplo: 'agency', 'mission', 'spacecraft'
```
 
 
---

## **Solución Ejercicio 2: Exportar tareas completadas a un archivo CSV**

```python
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def exportar_tareas_completadas_csv():
    hook = PostgresHook(postgres_conn_id='postgres_local')
    sql = "SELECT id, project_id, name, status, created_at FROM task WHERE status = 'completed';"
    registros = hook.get_records(sql)

    ruta_archivo = '/tmp/tareas_completadas.csv'

    with open(ruta_archivo, mode='w', newline='') as f:
        writer = csv.writer(f)
        # Escribir encabezados
        writer.writerow(['id', 'project_id', 'name', 'status', 'created_at'])

        # Escribir datos
        for r in registros:
            writer.writerow(r)

    print(f"Se exportaron {len(registros)} tareas completadas a {ruta_archivo}")

with DAG('dag_exportar_tareas_completadas',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    exportar = PythonOperator(
        task_id='exportar_tareas_completadas',
        python_callable=exportar_tareas_completadas_csv
    )
```

---

### ✅ Resultado

El archivo generado será:

```
/tmp/tareas_completadas.csv
```

Con columnas: `id`, `project_id`, `name`, `status`, `created_at`.
 