## **Ejercicio 1: Descargar imágenes de lanzamientos espaciales recientes**

### Concepto:

- https://thespacedevs.com/llapi
- https://ll.thespacedevs.com/docs/ 

Crear un DAG llamado `download_rocket_launches` que:

* Descargue un archivo JSON desde la API pública de lanzamientos espaciales.
* Extraiga las URLs de imágenes y descargue las fotos de los lanzamientos recientes.
* Muestre cuántas imágenes fueron descargadas correctamente.

Este DAG es un excelente ejemplo de orquestación de tareas ETL:

1. **Extracción** de datos desde una API externa (Space Devs API).
2. **Transformación** al seleccionar solo las URLs de imágenes.
3. **Carga** de datos al descargar las imágenes en un directorio local.

### Código:

```python
import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag,
)

def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator(
    task_id="get_pictures", python_callable=_get_pictures, dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify
```

### Diagrama:

```
[download_launches] ---> [get_pictures] ---> [notify]
```
 

## **Ejercicio 2: Listar datos desde PostgreSQL con PostgresHook**

### Concepto:

Crear un DAG llamado `dag_listar_proyectos` que:

* Se conecte a una base de datos PostgreSQL usando `PostgresHook`.
* Ejecute una consulta SQL para obtener todos los registros de la tabla `project`.
* Imprima por consola (logs de Airflow) los datos de cada proyecto: ID, nombre, estado y fecha de creación.


---

### Código:

```python
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def listar_proyectos():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    registros = postgres_hook.get_records("SELECT id, name, status, created_at FROM project")

    print("Proyectos encontrados:")
    for r in registros:
        print(f"- ID: {r[0]}, Nombre: {r[1]}, Estado: {r[2]}, Fecha: {r[3]}")

with DAG('dag_listar_proyectos',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    listar_task = PythonOperator(
        task_id='listar_proyectos',
        python_callable=listar_proyectos
    )
```

---

### SQL para crear la tabla de prueba:

Ejecuta esto en tu base de datos antes de correr el DAG:

```sql
CREATE TABLE IF NOT EXISTS project (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO project (name, status)
VALUES 
    ('Data Pipeline A', 'active'),
    ('ETL Workflow B', 'inactive'),
    ('Analytics Job C', 'active');
```

---

### Diagrama:

```
[listar_proyectos]
```
 