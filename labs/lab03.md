## **Ejercicio 1: Comunicación entre tareas usando XComs**

### Concepto:
Crear un DAG llamado `dag_ejercicio_xcom` que:

- Tenga una tarea inicial `push` que envíe un mensaje usando `xcom_push`.
- Tenga una segunda tarea `pull` que recupere ese mensaje con `xcom_pull` e imprima su contenido.

### Código:
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push(**kwargs):
    kwargs['ti'].xcom_push(key='mensaje', value='Hola desde XCom')

def pull(**kwargs):
    valor = kwargs['ti'].xcom_pull(task_ids='push', key='mensaje')
    print(f"Mensaje recibido: {valor}")

with DAG('dag_ejercicio_xcom', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='push', python_callable=push, provide_context=True)
    t2 = PythonOperator(task_id='pull', python_callable=pull, provide_context=True)

    t1 >> t2
```

### Diagrama:

```
[push] ---> [pull]
```

---

## **Ejercicio 2: Parametrización con Variables**

### Concepto:

Crear un DAG llamado `dag_ejercicio_variables` que:

* Obtenga el valor de una Variable llamada `nombre_usuario`.
* Imprima el mensaje `"Bienvenido, <nombre_usuario>"`.

> Tip: la Variable debe crearse previamente desde la UI (Admin > Variables).

### Código:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def saludo(**kwargs):
    nombre = Variable.get('nombre_usuario')
    print(f"Bienvenido, {nombre}")

with DAG('dag_ejercicio_variables', start_date=datetime(2025, 5, 1), schedule_interval=None, catchup=False) as dag:
    saludo_task = PythonOperator(task_id='saludo', python_callable=saludo, provide_context=True)
```

### Diagrama:

```
[saludo]
```

---

## **Ejercicio 3: Integración de XComs y Variables para simular una API**

### Concepto:

Crear un DAG llamado `dag_caso_real_api` que realice una extracción real de datos desde una API pública y procese la información obtenida:

1. La primera tarea `extraer` debe realizar una llamada HTTP a la URL `https://jsonplaceholder.typicode.com/users` y guardar la respuesta en un XCom.
2. La segunda tarea `procesar` debe recuperar los datos del XCom e imprimir el nombre y el correo electrónico de cada usuario recibido.


### Código:

```python
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

```

### Diagrama:

```
[extraer] ---> [procesar]
```

 
