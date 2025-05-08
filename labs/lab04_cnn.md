## Tutorial: Configurar conexión a PostgreSQL (`postgres2`) en Airflow

### Contexto del contenedor

Estás ejecutando PostgreSQL con la siguiente configuración en tu `docker-compose.yml`:

```yaml
  postgres2:
    image: postgres:13
    environment:
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    ports:
      - "5433:5432"
```

Esto significa que:

* Airflow puede conectarse al servicio PostgreSQL usando:

  * **Host**: `postgres2` (si Airflow corre fuera del contenedor) o `postgres2` (si corre dentro de la misma red de Docker)
  * **Puerto**: `5432`
  * **Base de datos**: `testdb`
  * **Usuario**: `testuser`
  * **Contraseña**: `testpass`

---

###  Paso a paso: crear la conexión en Airflow

1. Abre la **UI de Airflow** en tu navegador (por ejemplo: [http://localhost:8080](http://localhost:8080)).

2. En el menú superior, selecciona:
   **Admin > Connections**

3. Haz clic en el botón **“+” (Add a new record)**.

4. Completa el formulario de conexión con los siguientes valores:

| Campo         | Valor               |
| ------------- | ------------------- |
| **Conn Id**   | `postgres_local`    |
| **Conn Type** | `Postgres`          |
| **Host**      | `postgres2`         |
| **Database**  | `testdb`            |
| **Login**     | `testuser`          |
| **Password**  | `testpass`          |
| **Port**      | `5432`              |
| **Extra**     | *(dejar en blanco)* |

>  Asegúrate de usar `postgres2` si estás corriendo Airflow desde tu máquina host.
> Si estás usando **Docker Compose** y Airflow está en la **misma red que postgres2**, puedes usar `postgres2` como **host** y el puerto por defecto `5432`.

---

###  Validación

Puedes verificar que la conexión funciona ejecutando un DAG de prueba que use esta conexión y consulte, por ejemplo:

```sql
SELECT 1;
```

---

