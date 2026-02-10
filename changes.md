# Registro de Cambios - Febrero 2026

Este archivo resume las modificaciones realizadas para estabilizar y ejecutar el proyecto en un entorno Windows con Docker.

## Cambios en Archivos de Configuración

### `docker-compose.yml`
*   **Corrección de PYTHONPATH**: Se cambió el valor de la variable de entorno `PYTHONPATH` de `/opt/airflow/src:${PYTHONPATH}` a solo `/opt/airflow/src` para evitar advertencias de variables no definidas en Windows.
*   **Desactivación de montaje de config**: Se comentó la línea `- ./config/airflow.cfg:/opt/airflow/airflow.cfg`. Esto evita el error de inicialización donde Docker creaba una carpeta vacía en lugar de un archivo, bloqueando el arranque de Airflow.
*   **Estructura de Directorios**: Se aseguró la creación de las carpetas locales `dags`, `logs`, `plugins`, `config` y `src` para el correcto mapeo de volúmenes.

## Infraestructura y Base de Datos

*   **Limpieza de Directorios**: Se eliminó recursivamente la carpeta errónea `config/airflow.cfg/` que impedía el inicio del servidor.
*   **Inicialización de Airflow**:
    *   Ejecución de `airflow db init` para configurar el esquema de base de datos en el contenedor de Postgres.
    *   Creación de usuario administrador inicial:
        *   **Usuario**: `admin`
        *   **Contraseña**: `admin`
        *   **Rol**: `Admin`

## Entorno de Ejecución Python

*   **Análisis de Dependencias**: Se identificó que el script `lambda.py` requiere `boto3` para interactuar con AWS Secrets Manager, dependencia que no estaba explícitamente en el `requirements.txt` pero es necesaria para la lógica de conexión actual.

## Estado del Proyecto
*   **Airflow**: Operativo en [http://localhost:8080](http://localhost:8080).
*   **Contenedores**: Postgres, Scheduler y Webserver corriendo correctamente.
