## **Ejercicio 1: Descargar imágenes de lanzamientos espaciales recientes (usando HttpHook)**

### Concepto:

Vamos a crear un DAG llamado `download_rocket_launches` que:

1. Utilice un `HttpHook` para consultar la API pública de lanzamientos espaciales.
2. Extraiga las URLs de imágenes de los lanzamientos.
3. Descargue las imágenes a una carpeta local.
4. Notifique cuántas imágenes fueron descargadas.

> Este ejemplo representa un flujo ETL real: extracción desde API, transformación (filtrado) y carga de archivos.

### Requisitos previos:

Creá una conexión en Airflow:

* **ID:** `space_api`
* **Tipo:** HTTP
* **Host:** `https://ll.thespacedevs.com`

---

### Código actualizado:

```python
import json
import pathlib
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.http.hooks.http import HttpHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="download_rocket_launches",
    description="Download rocket images using HttpHook and DAG orchestration.",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "api", "images"],
) as dag:

    def descargar_json():
        hook = HttpHook(http_conn_id='space_api', method='GET')
        response = hook.run(endpoint='/2.0.0/launch/upcoming')
        response.raise_for_status()

        pathlib.Path("/opt/airflow/tmp").mkdir(parents=True, exist_ok=True)
        with open("/opt/airflow/tmp/launches.json", "w") as f:
            f.write(response.text)

    def descargar_imagenes():
        image_dir = "/opt/airflow/tmp/images"
        pathlib.Path(image_dir).mkdir(parents=True, exist_ok=True)

        with open("/opt/airflow/tmp/launches.json") as f:
            launches = json.load(f)
            urls = [l["image"] for l in launches["results"]]

        for url in urls:
            try:
                image_hook = HttpHook(method="GET")
                response = image_hook.run(url)
                filename = url.split("/")[-1]
                with open(f"{image_dir}/{filename}", "wb") as img:
                    img.write(response.content)
                print(f" {url}")
            except Exception as e:
                print(f" Error descargando {url}: {e}")

    t1 = PythonOperator(
        task_id="descargar_datos_lanzamientos",
        python_callable=descargar_json,
    )

    t2 = PythonOperator(
        task_id="descargar_imagenes",
        python_callable=descargar_imagenes,
    )

    t3 = BashOperator(
        task_id="notificar_cantidad",
        bash_command='echo "Imágenes descargadas: $(ls /opt/airflow/tmp/images/ | wc -l)"',
    )

    t1 >> t2 >> t3
```

### Diagrama:

```
[descargar_datos_lanzamientos] ---> [descargar_imagenes] ---> [notificar_cantidad]
```

 