"""
writing.py — Módulo de escritura (persistencia).

Contiene el DatabaseManager y la lógica de inserción con detección
de duplicados, reutilizando la lógica original de lambda.py.

La conexión a la base de datos usa variables de entorno directas
(la misma BD Postgres que levanta docker-compose para Airflow).
"""

import logging
import os
from typing import Dict, List, Tuple

import pandas as pd

logger = logging.getLogger("ani_scraping.writing")


# ---------------------------------------------------------------------------
# Configuración de conexión
# ---------------------------------------------------------------------------
def _get_db_config() -> dict:
    """Lee la configuración de BD desde variables de entorno."""
    return {
        "dbname": os.environ.get("DB_NAME", "airflow"),
        "user": os.environ.get("DB_USERNAME", "airflow"),
        "password": os.environ.get("DB_PASSWORD", "airflow"),
        "host": os.environ.get("DB_HOST", "postgres"),
        "port": int(os.environ.get("DB_PORT", 5432)),
    }


# ---------------------------------------------------------------------------
# DatabaseManager
# ---------------------------------------------------------------------------
class DatabaseManager:
    """Gestiona la conexión y operaciones sobre PostgreSQL."""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        import psycopg2

        try:
            config = _get_db_config()
            self.connection = psycopg2.connect(**config)
            self.cursor = self.connection.cursor()
            logger.info("Conexión a BD establecida (%s@%s/%s)", config["user"], config["host"], config["dbname"])
            return True
        except Exception as e:
            logger.error("Error de conexión a BD: %s", e)
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Conexión a BD cerrada.")

    def execute_query(self, query: str, params=None):
        if not self.cursor:
            raise RuntimeError("Base de datos no conectada")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def execute_ddl(self, ddl: str):
        """Ejecuta sentencias DDL (CREATE TABLE, etc.)."""
        if not self.cursor:
            raise RuntimeError("Base de datos no conectada")
        self.cursor.execute(ddl)
        self.connection.commit()

    def bulk_insert(self, df: pd.DataFrame, table_name: str) -> int:
        """Realiza una inserción masiva de un DataFrame a la tabla especificada."""
        if not self.connection or not self.cursor:
            raise RuntimeError("Base de datos no conectada")

        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]

            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            return len(df)
        except Exception as e:
            self.connection.rollback()
            raise RuntimeError(f"Error insertando en {table_name}: {e}") from e


# ---------------------------------------------------------------------------
# Inserción de componentes de regulación
# ---------------------------------------------------------------------------
def insert_regulations_component(db_manager: DatabaseManager, new_ids: List) -> Tuple[int, str]:
    """Inserta los componentes de las regulaciones."""
    if not new_ids:
        return 0, "No se proporcionaron IDs de regulación nuevos"

    try:
        id_rows = pd.DataFrame(new_ids, columns=["regulations_id"])
        id_rows["components_id"] = 7

        inserted_count = db_manager.bulk_insert(id_rows, "regulations_component")
        return inserted_count, f"Insertados {inserted_count} componentes de regulación"

    except Exception as e:
        return 0, f"Error insertando componentes de regulación: {e}"


# ---------------------------------------------------------------------------
# Inserción de registros con detección de duplicados
# ---------------------------------------------------------------------------
ENTITY_VALUE = "Agencia Nacional de Infraestructura"


def insert_new_records(db_manager: DatabaseManager, df: pd.DataFrame, entity: str) -> Tuple[int, str]:
    """
    Inserta nuevos registros en la base de datos evitando duplicados.
    Lógica de dedup idéntica al código original.
    """
    regulations_table_name = "regulations"

    try:
        # 1. OBTENER REGISTROS EXISTENTES
        query = """
            SELECT title, created_at, entity, COALESCE(external_link, '') as external_link
            FROM {}
            WHERE entity = %s
        """.format(regulations_table_name)

        existing_records = db_manager.execute_query(query, (entity,))

        if not existing_records:
            db_df = pd.DataFrame(columns=["title", "created_at", "entity", "external_link"])
        else:
            db_df = pd.DataFrame(existing_records, columns=["title", "created_at", "entity", "external_link"])

        logger.info("Registros existentes en BD para %s: %d", entity, len(db_df))

        # 2. PREPARAR DATAFRAME DE LA ENTIDAD
        entity_df = df[df["entity"] == entity].copy()

        if entity_df.empty:
            return 0, f"Sin registros para entidad {entity}"

        logger.info("Registros a procesar para %s: %d", entity, len(entity_df))

        # 3. NORMALIZAR DATOS PARA COMPARACIÓN CONSISTENTE
        if not db_df.empty:
            db_df["created_at"] = db_df["created_at"].astype(str)
            db_df["external_link"] = db_df["external_link"].fillna("").astype(str)
            db_df["title"] = db_df["title"].astype(str).str.strip()

        entity_df["created_at"] = entity_df["created_at"].astype(str)
        entity_df["external_link"] = entity_df["external_link"].fillna("").astype(str)
        entity_df["title"] = entity_df["title"].astype(str).str.strip()

        # 4. IDENTIFICAR DUPLICADOS
        logger.info("=== INICIANDO VALIDACIÓN DE DUPLICADOS ===")
        duplicates_found = 0
        internal_duplicates = 0

        if db_df.empty:
            new_records = entity_df.copy()
            logger.info("No hay registros existentes, todos son nuevos")
        else:
            entity_df["unique_key"] = (
                entity_df["title"] + "|" +
                entity_df["created_at"] + "|" +
                entity_df["external_link"]
            )
            db_df["unique_key"] = (
                db_df["title"] + "|" +
                db_df["created_at"] + "|" +
                db_df["external_link"]
            )

            existing_keys = set(db_df["unique_key"])
            entity_df["is_duplicate"] = entity_df["unique_key"].isin(existing_keys)

            new_records = entity_df[~entity_df["is_duplicate"]].copy()
            duplicates_found = len(entity_df) - len(new_records)

            if duplicates_found > 0:
                logger.info("Duplicados encontrados: %d", duplicates_found)

        # 5. REMOVER DUPLICADOS INTERNOS
        before = len(new_records)
        new_records = new_records.drop_duplicates(
            subset=["title", "created_at", "external_link"],
            keep="first",
        )
        internal_duplicates = before - len(new_records)
        if internal_duplicates > 0:
            logger.info("Duplicados internos removidos: %d", internal_duplicates)

        total_duplicates = duplicates_found + internal_duplicates
        logger.info("=== DUPLICADOS IDENTIFICADOS: %d ===", total_duplicates)

        if new_records.empty:
            return 0, f"Sin registros nuevos para {entity} tras validación de duplicados"

        # 6. LIMPIAR COLUMNAS AUXILIARES
        for col in ("unique_key", "is_duplicate"):
            if col in new_records.columns:
                new_records = new_records.drop(columns=[col])

        logger.info("Registros finales a insertar: %d", len(new_records))

        # 7. INSERTAR
        try:
            logger.info("=== INSERTANDO %d REGISTROS ===", len(new_records))
            total_rows = db_manager.bulk_insert(new_records, regulations_table_name)

            if total_rows == 0:
                return 0, f"No se insertaron registros para {entity}"

            logger.info("Registros insertados exitosamente: %d", total_rows)

        except Exception as insert_error:
            logger.error("Error en inserción: %s", insert_error)
            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                logger.warning("Error de duplicados — algunos registros ya existían")
                return 0, f"Algunos registros de {entity} eran duplicados y se omitieron"
            raise

        # 8. OBTENER IDS DE REGISTROS INSERTADOS
        new_ids_query = f"""
            SELECT id FROM {regulations_table_name}
            WHERE entity = %s
            ORDER BY id DESC
            LIMIT %s
        """
        new_ids_result = db_manager.execute_query(new_ids_query, (entity, total_rows))
        new_ids = [row[0] for row in new_ids_result]

        logger.info("IDs obtenidos: %d", len(new_ids))

        # 9. INSERTAR COMPONENTES
        inserted_comp = 0
        comp_msg = ""
        if new_ids:
            try:
                inserted_comp, comp_msg = insert_regulations_component(db_manager, new_ids)
                logger.info("Componentes: %s", comp_msg)
            except Exception as comp_error:
                logger.error("Error insertando componentes: %s", comp_error)
                comp_msg = f"Error insertando componentes: {comp_error}"

        # 10. RESULTADO
        stats = (
            f"Procesados: {len(entity_df)} | "
            f"Existentes: {len(db_df)} | "
            f"Duplicados omitidos: {total_duplicates} | "
            f"Nuevos insertados: {total_rows}"
        )
        message = f"Entidad {entity}: {stats}. {comp_msg}"
        logger.info("=== RESULTADO FINAL ===")
        logger.info(message)

        return total_rows, message

    except Exception as e:
        if hasattr(db_manager, "connection") and db_manager.connection:
            db_manager.connection.rollback()
        error_msg = f"Error procesando entidad {entity}: {e}"
        logger.error("ERROR CRÍTICO: %s", error_msg)
        import traceback
        logger.error(traceback.format_exc())
        return 0, error_msg


# ---------------------------------------------------------------------------
# Punto de entrada del módulo
# ---------------------------------------------------------------------------
def run_writing(records: List[Dict]) -> int:
    """
    Ejecuta la escritura de registros validados en la base de datos.

    Args:
        records: Registros validados listos para insertar.

    Returns:
        Número de filas insertadas.
    """
    if not records:
        logger.warning("No hay registros para escribir.")
        return 0

    df = pd.DataFrame(records)
    logger.info("Total de registros a escribir: %d", len(df))

    db_manager = DatabaseManager()
    if not db_manager.connect():
        raise RuntimeError("No se pudo conectar a la base de datos")

    try:
        inserted_count, message = insert_new_records(db_manager, df, ENTITY_VALUE)
        logger.info("Escritura finalizada — filas insertadas: %d", inserted_count)
        return inserted_count
    finally:
        db_manager.close()
