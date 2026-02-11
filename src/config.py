"""
config.py — Configuración centralizada con pydantic-settings.

Valida tipos y valores de las variables de entorno al importar.
Si falta una variable obligatoria o tiene un tipo incorrecto,
la aplicación falla inmediatamente con un mensaje claro.
"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Variables de entorno del pipeline ANI."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Base de datos -------------------------------------------------------
    db_name: str = "airflow"
    db_username: str = "airflow"
    db_password: str = "airflow"
    db_host: str = "postgres"
    db_port: int = 5432

    # --- Aplicación ----------------------------------------------------------
    ani_num_pages: int = 9
    validation_rules_path: str = str(
        Path(__file__).resolve().parent.parent / "configs" / "validation_rules.yaml"
    )
    init_sql_path: str = "/opt/airflow/configs/init.sql"

    @property
    def db_config(self) -> dict:
        """Retorna la configuración de conexión para psycopg2."""
        return {
            "dbname": self.db_name,
            "user": self.db_username,
            "password": self.db_password,
            "host": self.db_host,
            "port": self.db_port,
        }


settings = Settings()
