"""
validation.py — Módulo de validación configurable.

Intercala una etapa de validación entre extracción y escritura.
Las reglas de tipo, regex y obligatoriedad se leen de un archivo YAML
ubicado en configs/validation_rules.yaml.

Comportamiento:
  - Campo que no cumple → queda como None.
  - Fila cuyo(s) campo(s) obligatorio(s) no cumple(n) → se descarta entera.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

import yaml

from config import settings

logger = logging.getLogger("ani_scraping.validation")

# ---------------------------------------------------------------------------
# Ruta por defecto del archivo de reglas
# ---------------------------------------------------------------------------
_DEFAULT_RULES_PATH = settings.validation_rules_path


# ---------------------------------------------------------------------------
# Carga de reglas
# ---------------------------------------------------------------------------
def load_rules(path: Optional[str] = None) -> Dict:
    """Carga las reglas de validación desde un archivo YAML."""
    rules_path = path or _DEFAULT_RULES_PATH
    logger.info("Cargando reglas de validación desde: %s", rules_path)

    with open(rules_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    fields = config.get("fields", {})
    logger.info("Reglas cargadas para %d campos: %s", len(fields), list(fields.keys()))
    return fields


# ---------------------------------------------------------------------------
# Validación de un campo individual
# ---------------------------------------------------------------------------
_TYPE_MAP = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
}


def _validate_field(value, rule: Dict) -> Tuple:
    """
    Valida un valor según su regla.

    Returns:
        (is_valid, cleaned_value)
        - is_valid=True  → el valor cumple; cleaned_value es el mismo valor.
        - is_valid=False → el valor NO cumple; cleaned_value es None.
    """
    # Si el valor es None / vacío, solo importa la obligatoriedad (se maneja afuera)
    if value is None:
        return False, None

    # --- Verificar tipo ---
    expected_type_name = rule.get("type")
    if expected_type_name:
        expected_type = _TYPE_MAP.get(expected_type_name)
        if expected_type and not isinstance(value, expected_type):
            # Intentar castear
            try:
                value = expected_type(value)
            except (ValueError, TypeError):
                return False, None

    # --- Verificar regex ---
    regex = rule.get("regex")
    if regex and isinstance(value, str):
        if not re.match(regex, value):
            return False, None

    return True, value


# ---------------------------------------------------------------------------
# Validación de un registro completo
# ---------------------------------------------------------------------------
def validate_record(record: Dict, rules: Dict) -> Optional[Dict]:
    """
    Valida un registro individual.

    Returns:
        Registro limpio (campos inválidos → None) o None si un campo
        obligatorio no cumple.
    """
    cleaned = dict(record)  # copia superficial

    for field_name, rule in rules.items():
        value = cleaned.get(field_name)
        is_valid, new_value = _validate_field(value, rule)

        if not is_valid:
            required = rule.get("required", False)
            if required:
                # Campo obligatorio no cumple → descartar fila
                return None
            # Campo opcional no cumple → dejar como None
            cleaned[field_name] = None
        else:
            cleaned[field_name] = new_value

    return cleaned


# ---------------------------------------------------------------------------
# Punto de entrada del módulo
# ---------------------------------------------------------------------------
def run_validation(records: List[Dict], rules_path: Optional[str] = None) -> List[Dict]:
    """
    Ejecuta la validación sobre una lista de registros.

    Args:
        records: Registros crudos provenientes de la extracción.
        rules_path: Ruta opcional al archivo YAML de reglas.

    Returns:
        Lista de registros que pasaron la validación.
    """
    rules = load_rules(rules_path)

    total = len(records)
    valid_records = []  # type: List[Dict]
    discarded = 0

    for record in records:
        result = validate_record(record, rules)
        if result is None:
            discarded += 1
        else:
            valid_records.append(result)

    logger.info(
        "Validación finalizada — recibidos: %d | descartados: %d | válidos: %d",
        total, discarded, len(valid_records),
    )
    return valid_records
