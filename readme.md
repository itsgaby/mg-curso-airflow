#  Curso Apache Airflow - Entorno en GitHub Codespaces

Este repositorio contiene el entorno listo para ejecutar **Apache Airflow** en la nube usando **GitHub Codespaces**. No necesitas instalar nada en tu computadora.

> Ideal para estudiantes del curso de Airflow que quieren desarrollar, probar y visualizar DAGs desde el navegador.

---

##  ¿Qué es GitHub Codespaces?

GitHub Codespaces es un entorno de desarrollo en la nube. Este repositorio incluye una configuración que levanta Apache Airflow con Docker y te permite acceder a su interfaz web directamente desde el navegador.

---

##  Requisitos

- Una cuenta de GitHub.
- Acceso a [GitHub Codespaces](https://github.com/features/codespaces) (gratis hasta 60 horas por mes).
- Navegador web moderno.

---

##  Cómo usar este entorno (paso a paso)

### 1. Abrí un Codespace

1. Entrá a este repositorio (o tu fork si preferís).
2. Hacé clic en el botón verde `<> Code`.
3. Seleccioná la pestaña **"Codespaces"**.
4. Hacé clic en **“Create codespace on main”**.

>  Esperá unos minutos mientras se configura el entorno.

---

### 2. Inicializar Airflow

Cuando se abra Codespaces, hacé lo siguiente:

1. Abrí la terminal integrada (`Ctrl+Shift+ñ` o "Terminal > New Terminal").
2. Ejecutá estos comandos:

```bash
# Inicializar la base de datos de Airflow (solo la primera vez)
docker compose up airflow-init

# Iniciar los servicios
docker compose up
```

---

### 3. Accedé a la interfaz web de Airflow

1. Hacé clic en el ícono `Ports` en el panel izquierdo de GitHub Codespaces (🔌).
2. Buscá el puerto `8080`, y hacé clic en **"Open in Browser"**.
3. Ingresá con estas credenciales:

```
Usuario: airflow
Contraseña: airflow
```

---

### 4. Agregá tus propios DAGs

- Los DAGs están en la carpeta `/dags`.
- Podés crear archivos nuevos `.py` o editar los existentes.
- Airflow detectará automáticamente los cambios.

Ejemplo rápido (ya incluido en este repo):

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('hola_mundo', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    tarea = BashOperator(
        task_id='imprimir_mensaje',
        bash_command='echo "Hola mundo desde Airflow!"'
    )
```

---

### 5. Detener y reiniciar Airflow

Para detener:

```bash
docker compose down
```

Para reiniciar:

```bash
docker compose up
```

---

###  ¿Querés reiniciar todo desde cero?

Si querés eliminar todos los datos y reiniciar desde cero:

```bash
docker compose down --volumes --remove-orphans
```

---

##  Estructura del Proyecto

```
.
├── dags/               # DAGs del curso
├── docker-compose.yaml # Configuración de servicios
├── .env                # UID para evitar errores de permisos
├── .devcontainer/      # Configuración para Codespaces
│   └── Dockerfile
└── devcontainer.json   # Entradas necesarias para el entorno remoto
```

---

##  ¿Dudas o errores?

1. Verificá que los contenedores estén corriendo:

```bash
docker compose ps
```

2. Revisá los logs:

```bash
docker compose logs
```

3. Preguntá a tu profesor o en el foro del curso.

---

¡Listo! Ya estás trabajando con Airflow desde la nube 

# Nota:
```bash
# Comandos utiles
docker-compose restart airflow-scheduler
docker-compose logs airflow-worker
docker-compose exec airflow-webserver airflow dags list
docker-compose logs airflow-scheduler
docker-compose exec airflow-webserver airflow dags list-import-errors


```

# Probar Airflow 3.0.0 (versión nueva)

Si querés probar la nueva versión de Apache Airflow 3.0.0, este repositorio ya incluye una configuración lista para eso.

## 1. Cambiá el archivo `docker-compose`

1. Abrí la terminal integrada.
2. Ejecutá el siguiente comando para usar la nueva configuración:

```bash
cp docker-compose-3.0.0.yaml docker-compose.yaml
```

> Esto reemplaza la versión por defecto (2.7.3) con la nueva (3.0.0).

---

## 2. Limpiá recursos anteriores (opcional pero recomendado)

Antes de iniciar Airflow 3.0.0, te sugerimos limpiar volúmenes, contenedores e imágenes para evitar conflictos:

```bash
# Elimina contenedores, volúmenes y servicios huérfanos
docker compose down --volumes --remove-orphans

# Borra imágenes no utilizadas
docker image prune -f
```

Si querés borrar la imagen anterior (por ejemplo, 2.7.3) manualmente:

```bash
docker rmi apache/airflow:2.7.3
```

---

## 3. Inicializá y levantá Airflow 3.0.0

```bash
docker compose up airflow-init
docker compose up
```

---

## 4. Accedé a la nueva interfaz

* Hacé clic en el ícono `Ports` en Codespaces.
* Abrí el puerto `8080` en el navegador.
* Ingresá con:

```
Usuario: airflow
Contraseña: airflow
```

---

## 5. ¿Qué cambia en Airflow 3.0.0?

* Se usa un nuevo servicio: `airflow-apiserver`.
* Aparece un nuevo componente: `airflow-dag-processor`.
* Cambia el sistema de autenticación a `FabAuthManager`.
* Redis se actualiza a la versión `7.2-bookworm`.

Más detalles en las [notas de la versión oficial](https://github.com/apache/airflow/releases).
 