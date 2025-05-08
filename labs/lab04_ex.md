## **Ejercicio 4: Descargar datos de otra entidad de SpaceDevs API**

### Consigna:

Crear un DAG llamado `download_entity_data` que:

1. Permita seleccionar **otra entidad** distinta a `launch` de la API pública de SpaceDevs (por ejemplo: `astronaut`, `agency`, `mission`, etc.).  
   Referencia oficial: [https://ll.thespacedevs.com/docs/#/](https://ll.thespacedevs.com/docs/#/)
2. Use un `BashOperator` o `PythonOperator` para hacer una llamada HTTP al endpoint correspondiente.
3. Guarde la respuesta (en formato JSON) en un archivo local dentro del directorio `/tmp/`, por ejemplo `/tmp/astronauts.json`.

> Tip: Asegúrate de manejar errores si el endpoint no responde correctamente.

### Diagrama:

```

[descargar_datos_entidad]

```

## Ejercicio 2: Exportar tareas completadas a un archivo

### Consigna:

Crear un DAG llamado `dag_exportar_tareas_completadas` que:

1. Leer todas las tareas con estado `"completed"` desde la tabla `task`.
2. Guarde los resultados en un archivo JSON en la ruta `/tmp/tareas_completadas.json`.
3. Imprima en los logs la cantidad de tareas exportadas.

> La conexión debe estar configurada como `postgres_local` en Admin > Connections.

> Asegúrate de que la tabla `task` esté creada y contenga datos (puedes usar los scripts SQL del ejercicio anterior).

### Diagrama:

```
[exportar_tareas_completadas]
```
 