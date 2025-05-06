## **Desafío Final: Unir posts y comentarios en un reporte**

### Consigna:
Crear un DAG llamado `dag_reporte_posts_comentarios` que:

1. Tenga dos tareas en paralelo:
   - `descargar_posts`: descarga la lista de publicaciones desde `https://jsonplaceholder.typicode.com/posts`.
   - `descargar_comentarios`: descarga los comentarios desde `https://jsonplaceholder.typicode.com/comments`.

2. Una tercera tarea `reporte` debe esperar a ambas y luego:
   - Relacionar posts y comentarios usando el campo `postId`.
   - Mostrar para los primeros 3 posts un resumen con su título y los primeros 2 comentarios asociados.

### Diagrama:
```

[descargar_posts] 
                                ---> [reporte]
[descargar_comentarios]/

```
