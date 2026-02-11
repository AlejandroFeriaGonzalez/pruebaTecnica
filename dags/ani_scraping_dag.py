"""
ani_scraping_dag.py — DAG de Airflow para el scraping de normativas ANI.

Secuencia:
  create_tables → extract → validate → write

Comunicación entre tareas vía XCom (JSON).
"""

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Argumentos por defecto del DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------------
# Número de páginas a scrapear (configurable via variable de Airflow)
# ---------------------------------------------------------------------------
NUM_PAGES = int(os.environ.get("ANI_NUM_PAGES", 9))

# Ruta al archivo de reglas de validación dentro del contenedor
RULES_PATH = os.environ.get(
    "VALIDATION_RULES_PATH",
    "/opt/airflow/configs/validation_rules.yaml",
)

# Ruta al archivo DDL
INIT_SQL_PATH = os.environ.get(
    "INIT_SQL_PATH",
    "/opt/airflow/configs/init.sql",
)


# ---------------------------------------------------------------------------
# Funciones de las tareas
# ---------------------------------------------------------------------------
def task_create_tables(**context):
    """Crea las tablas si no existen, ejecutando init.sql."""
    import logging

    from writing import DatabaseManager

    logger = logging.getLogger("ani_scraping.create_tables")

    # Buscar init.sql en varias ubicaciones posibles
    sql_path = INIT_SQL_PATH
    if not os.path.exists(sql_path):
        # Fallback: buscar en configs/
        alt_path = "/opt/airflow/configs/init.sql"
        if os.path.exists(alt_path):
            sql_path = alt_path
        else:
            logger.warning("No se encontró init.sql — las tablas deben existir previamente.")
            return

    with open(sql_path, "r", encoding="utf-8") as f:
        ddl = f.read()

    db = DatabaseManager()
    if not db.connect():
        raise RuntimeError("No se pudo conectar a la BD para crear tablas")

    try:
        db.execute_ddl(ddl)
        logger.info("Tablas creadas / verificadas exitosamente.")
    finally:
        db.close()


def task_extract(**context):
    """Extrae datos del sitio de la ANI."""
    from extraction import run_extraction

    records = run_extraction(num_pages=NUM_PAGES)

    # Serializar a JSON para XCom
    context["ti"].xcom_push(key="raw_records", value=json.dumps(records, default=str))
    return len(records)


def task_validate(**context):
    """Valida los registros extraídos según reglas YAML."""
    from validation import run_validation

    # Recuperar datos del paso anterior
    raw_json = context["ti"].xcom_pull(task_ids="extract", key="raw_records")
    records = json.loads(raw_json)

    validated = run_validation(records, rules_path=RULES_PATH)

    # Serializar para el siguiente paso
    context["ti"].xcom_push(key="validated_records", value=json.dumps(validated, default=str))
    return len(validated)


def task_write(**context):
    """Escribe los registros validados en PostgreSQL."""
    from writing import run_writing

    # Recuperar datos validados
    validated_json = context["ti"].xcom_pull(task_ids="validate", key="validated_records")
    records = json.loads(validated_json)

    inserted = run_writing(records)
    return inserted


# ---------------------------------------------------------------------------
# Definición del DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ani_scraping",
    default_args=default_args,
    description="Scraping de normativas ANI: Extracción → Validación → Escritura",
    schedule_interval=None,  # Ejecución manual
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ani", "scraping", "normativas"],
) as dag:

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=task_create_tables,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    write = PythonOperator(
        task_id="write",
        python_callable=task_write,
    )

    # Secuencia: crear tablas → extraer → validar → escribir
    create_tables >> extract >> validate >> write
