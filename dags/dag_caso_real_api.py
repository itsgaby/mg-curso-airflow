from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests  # para hacer la llamada HTTP

def extraer(**kwargs):
    # Usamos directamente la URL como ejemplo fijo o desde Variable si preferís:
    # api_url = Variable.get('api_endpoint', default_var='https://jsonplaceholder.typicode.com/users')
    api_url = 'https://jsonplaceholder.typicode.com/users'
    
    response = requests.get(api_url)
    if response.status_code == 200:
        datos = response.json()
        print("Datos extraídos correctamente.")
        kwargs['ti'].xcom_push(key='usuarios', value=datos)
    else:
        raise Exception(f"Error al acceder a la API: {response.status_code}")

def procesar(**kwargs):
    usuarios = kwargs['ti'].xcom_pull(task_ids='extraer', key='usuarios')
    print("Procesando usuarios:")
    for user in usuarios:
        print(f"- {user['name']} ({user['email']})")

with DAG('dag_caso_real_api',
         start_date=datetime(2025, 5, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='extraer', python_callable=extraer, provide_context=True)
    t2 = PythonOperator(task_id='procesar', python_callable=procesar, provide_context=True)

    t1 >> t2
