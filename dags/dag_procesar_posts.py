from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests  # para hacer la llamada HTTP

def descargar_posts(**kwargs):
    # Usamos directamente la URL como ejemplo fijo o desde Variable si preferís:
    # api_url = Variable.get('api_endpoint', default_var='https://jsonplaceholder.typicode.com/users')
    api_url = 'https://jsonplaceholder.typicode.com/posts'
    
    response = requests.get(api_url)
    if response.status_code == 200:
        datos = response.json()
        print("Datos extraídos correctamente.")
        kwargs['ti'].xcom_push(key='posts', value=datos)
    else:
        raise Exception(f"Error al acceder a la API: {response.status_code}")

def mostrar_titulos(**kwargs):
    usuarios = kwargs['ti'].xcom_pull(task_ids='descargar_posts', key='posts')
    print("Procesando posts:")
    for user in usuarios[:5]:
        print(f"Titulos- {user['title']}")

with DAG('dag_procesar_posts',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='descargar_posts', python_callable=descargar_posts, provide_context=True)
    t2 = PythonOperator(task_id='mostrar_titulos', python_callable=mostrar_titulos, provide_context=True)

    t1 >> t2