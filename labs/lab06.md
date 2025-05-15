Gracias. Basándonos en el formato del archivo `lab04.md`, te reescribo el ejemplo de **subsystem task para un ETL complejo** siguiendo el mismo estilo estructurado.

---

## **Ejercicio 3: ETL complejo por subsistemas usando TaskGroup**

### Concepto:

Este DAG llamado `complex_sales_etl` simula un pipeline de procesamiento de datos de ventas dividido en subsistemas:

1. **Extracción:** descarga datos crudos de ventas.
2. **Transformación:** procesa, limpia y enriquece los datos.
3. **Carga:** almacena los datos en un almacén de datos.
4. **Chequeos de calidad:** valida la integridad.
5. **Notificación:** indica que el proceso finalizó exitosamente.

Se utiliza `TaskGroup` para encapsular las tareas del subsistema de transformación.

---

### Código:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


def extract_sales_data():
    print("Extrayendo datos crudos de ventas...")

def clean_data():
    print("Limpiando los datos...")

def enrich_data():
    print("Enriqueciendo con metadatos de clientes...")

def join_product_catalog():
    print("Unificando con catálogo de productos...")

def load_to_warehouse():
    print("Cargando en el almacén de datos...")

def run_quality_checks():
    print("Ejecutando validaciones de calidad...")

def send_notification():
    print("Proceso ETL finalizado. Notificando al equipo.")


with DAG(
    dag_id="complex_sales_etl",
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    description="ETL completo con subsistemas organizados por TaskGroup",
    tags=["etl", "taskgroup", "ventas"]
) as dag:

    extract = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data
    )

    with TaskGroup("transform_sales_data", tooltip="Transformaciones de ventas") as transform:
        clean = PythonOperator(
            task_id="clean_data",
            python_callable=clean_data
        )

        enrich = PythonOperator(
            task_id="enrich_data",
            python_callable=enrich_data
        )

        join_catalog = PythonOperator(
            task_id="join_product_catalog",
            python_callable=join_product_catalog
        )

        clean >> enrich >> join_catalog

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks
    )

    notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification
    )

    # Orquestación principal
    extract >> transform >> load >> quality >> notify
```

---

### Diagrama:

```
[extract_sales_data] ---> [transform_sales_data]
                             |--> clean_data
                             |--> enrich_data
                             |--> join_product_catalog
                         ---> [load_to_warehouse]
                         ---> [run_quality_checks]
                         ---> [send_notification]
```

---

¿Querés que lo adapte a un ejemplo con PostgreSQL, o que use archivos en S3 o GCS? También podemos armarlo con `@task` para mayor limpieza.
