## **Ejercicio 1: Múltiples valores con XComs**

### Consigna:

Crear un DAG llamado `dag_xcom_multiple` que:

* Tenga una tarea `generar_datos` que envíe dos valores distintos usando `xcom_push` con las claves `"mensaje"` y `"codigo"`.
* Tenga una tarea `mostrar_datos` que recupere ambos valores con `xcom_pull` e imprima un mensaje combinado.

### Diagrama:

```
[generar_datos] ---> [mostrar_datos]
```

---

## **Ejercicio 2: Uso de Variable con valor por defecto**

### Consigna:

Crear un DAG llamado `dag_variable_default` que:

* Lea una Variable llamada `entorno`.
* Si la Variable no existe, debe utilizar el valor por defecto `"desarrollo"`.
* Imprima el mensaje: `"Ambiente de ejecución: <entorno>"`.

> El DAG no debe fallar si la Variable no fue definida en la UI.

### Diagrama:

```
[imprimir_entorno]
```

---

## **Ejercicio 3: Descarga y procesamiento de posts desde API**

### Consigna:

Crear un DAG llamado `dag_procesar_posts` que:

1. La tarea `descargar_posts` debe hacer una llamada HTTP real a `https://jsonplaceholder.typicode.com/posts`.
2. Debe guardar la lista de posts en un XCom.
3. La tarea `mostrar_titulos` debe recuperar los datos y mostrar los títulos de los primeros 5 posts.

### Diagrama:

```
[descargar_posts] ---> [mostrar_titulos]
```

 