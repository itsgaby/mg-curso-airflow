## **Solución Ejercicio 1: Múltiples valores con XComs**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def generar_datos(**kwargs):
    kwargs['ti'].xcom_push(key='mensaje', value='Hola desde XCom')
    kwargs['ti'].xcom_push(key='codigo', value='El código es 1234')

def mostrar_datos(**kwargs):
    mensaje = kwargs['ti'].xcom_pull(task_ids='generar_datos', key='mensaje')
    codigo = kwargs['ti'].xcom_pull(task_ids='generar_datos', key='codigo')
    print(f"{mensaje} - {codigo}")

with DAG('dag_xcom_multiple',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='generar_datos', python_callable=generar_datos, provide_context=True)
    t2 = PythonOperator(task_id='mostrar_datos', python_callable=mostrar_datos, provide_context=True)

    t1 >> t2
```

---

## **Solución Ejercicio 2: Uso de Variable con valor por defecto**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def imprimir_entorno(**kwargs):
    entorno = Variable.get('entorno', default_var='desarrollo')
    print(f"Ambiente de ejecución: {entorno}")

with DAG('dag_variable_default',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='imprimir_entorno', python_callable=imprimir_entorno, provide_context=True)
```

---

## **Solución Ejercicio 3: Descarga y procesamiento de posts desde API**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def descargar_posts(**kwargs):
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    if response.status_code == 200:
        posts = response.json()
        kwargs['ti'].xcom_push(key='posts', value=posts)
    else:
        raise Exception(f"Error al obtener los posts: {response.status_code}")

def mostrar_titulos(**kwargs):
    posts = kwargs['ti'].xcom_pull(task_ids='descargar_posts', key='posts')
    print("Títulos de los primeros 5 posts:")
    for post in posts[:5]:
        print(f"- {post['title']}")

with DAG('dag_procesar_posts',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='descargar_posts', python_callable=descargar_posts, provide_context=True)
    t2 = PythonOperator(task_id='mostrar_titulos', python_callable=mostrar_titulos, provide_context=True)

    t1 >> t2
```

 