## **Ejercicio 1: Branching según el día de la semana**

### Concepto:

Crear un DAG llamado `branch_based_on_day` que:

* Se ejecute diariamente.
* Use un `BranchPythonOperator` para decidir qué tarea ejecutar según el **día de la semana del `execution_date`**.
* Si es **sábado o domingo**, ejecuta una tarea especial.
* Si no, ejecuta una tarea regular.

### Código:

```python
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def decide_what_to_run(**context):
    execution_date = context['execution_date']

    print("execution_date:", execution_date)
    print("weekday():", execution_date.weekday())
    print("day_of_week:", getattr(execution_date, 'day_of_week', 'N/A'))
    print("isoweekday():", execution_date.isoweekday())

    if execution_date.day_of_week > 4:  # 5 = sábado, 6 = domingo
        return 'special_task'
    else:
        return 'regular_task'

with DAG('branch_based_on_day',
         start_date=datetime(2025, 5, 1),
         schedule_interval='@daily',
         catchup=True) as dag:

    branch = BranchPythonOperator(
        task_id='branching',
        python_callable=decide_what_to_run,
        provide_context=True
    )

    special = EmptyOperator(task_id='special_task')
    regular = EmptyOperator(task_id='regular_task')
    join = EmptyOperator(task_id='join', trigger_rule='none_failed_min_one_success')

    branch >> [special, regular] >> join
```

---

## **Ejercicio 2: Subir archivos a FTP o PostgreSQL (`postgres2`)**

### Concepto:


Crear un DAG llamado `upload_launch_data` que:

Usando los datos descargados de los lanzamientos espaciales (como en `lab04`), el DAG elige condicionalmente el destino de salida:

* **FTP**: Subir el archivo `launches.json`.
* **PostgreSQL (`postgres2`)**: Insertar datos en la base de datos `testdb`.

### Código:

```python
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def choose_destination(**kwargs):
    return kwargs['params'].get('output_target', 'to_ftp')

def upload_to_ftp():
    hook = FTPHook(ftp_conn_id='ftp_default')
    hook.store_file('/launches.json', '/tmp/launches.json')

def upload_to_db():
    with open('/tmp/launches.json') as f:
        data = json.load(f)
    pg = PostgresHook(postgres_conn_id='postgres_local')
    for launch in data['results']:
        pg.run("INSERT INTO launches (id, name, net) VALUES (%s, %s, %s)",
               parameters=(launch['id'], launch['name'], launch['net']))

default_args = {'start_date': datetime(2025, 5, 1)}

with DAG('upload_launch_data',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    download = BashOperator(
        task_id='download_json',
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'"
    )

    choose = BranchPythonOperator(
        task_id='choose_target',
        python_callable=choose_destination,
        provide_context=True
    )

    to_ftp = PythonOperator(
        task_id='to_ftp',
        python_callable=upload_to_ftp
    )

    to_db = PythonOperator(
        task_id='to_db',
        python_callable=upload_to_db
    )

    end = BashOperator(task_id='done', bash_command='echo \"Data uploaded.\"')

    download >> choose >> [to_ftp, to_db] >> end
```

---

## **Requisitos previos**

### 1. SQL de inicialización para PostgreSQL (`postgres2`)

Crea este archivo como `./init/postgres2-init.sql`:

```sql
CREATE TABLE IF NOT EXISTS launches (
    id TEXT PRIMARY KEY,
    name TEXT,
    net TIMESTAMP
);
```

---

### 2. Configurar conexiones en Airflow

####  `ftp_default` – FTP Local (vía contenedor)

```bash
docker-compose exec airflow-webserver airflow connections add ftp_default \
    --conn-type ftp \
    --conn-host ftp \
    --conn-login airflow \
    --conn-password airflow \
    --conn-port 21
```

####  `postgres_test` – PostgreSQL en `postgres2`

```bash
docker-compose exec airflow-webserver airflow connections add postgres_local \
    --conn-type postgres \
    --conn-host postgres2 \
    --conn-login testuser \
    --conn-password testpass \
    --conn-port 5432 \
    --conn-schema testdb
```
 
---

### 3. Trigger the DAG with the database output

You can trigger it from the UI or using the CLI with a parameter override:

```bash
docker-compose exec airflow-webserver airflow dags trigger upload_launch_data \
    --conf '{"output_target": "to_db"}'
```

This will activate the branch that runs `upload_to_db`.

### 4. notes
```bash
docker-compose exec airflow-webserver airflow tasks clear -s 2025-05-01 -e 2025-05-15 branch_based_on_day
```
