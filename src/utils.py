"""
utils.py — Constantes compartidas y funciones auxiliares.

Contiene las constantes de configuración del scraping y funciones 
de limpieza/normalización reutilizadas por los módulos de extracción,
validación y escritura.
"""

import logging
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("ani_scraping")

# ---------------------------------------------------------------------------
# Constantes de scraping
# ---------------------------------------------------------------------------
ENTITY_VALUE = "Agencia Nacional de Infraestructura"
FIXED_CLASSIFICATION_ID = 13
URL_BASE = (
    r"https://www.ani.gov.co/informacion-de-la-ani/normatividad"
    r"?field_tipos_de_normas__tid=12&title=&body_value="
    r"&field_fecha__value%5Bvalue%5D%5Byear%5D="
)

# Clasificaciones de documentos
CLASSIFICATION_KEYWORDS = {
    "resolución": 15,
    "resolucion": 15,
    "decreto": 14,
}
DEFAULT_RTYPE_ID = 14


# ---------------------------------------------------------------------------
# Funciones auxiliares
# ---------------------------------------------------------------------------
def clean_quotes(text: str) -> str:
    """Elimina todos los tipos de comillas de un texto."""
    if not text:
        return text

    quotes_map = {
        "\u201C": "", "\u2018": "", "\u2019": "", "\u00AB": "", "\u00BB": "",
        "\u201E": "", "\u201A": "", "\u2039": "", "\u203A": "", '"': "",
        "'": "", "´": "", "`": "", "′": "", "″": "",
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)

    quotes_pattern = (
        r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB'
        r"\u201E\u201A\u2039\u203A\u2032\u2033]"
    )
    cleaned_text = re.sub(quotes_pattern, "", cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = " ".join(cleaned_text.split())
    return cleaned_text


def get_rtype_id(title: str) -> int:
    """Obtiene el rtype_id basado en el título del documento."""
    title_lower = title.lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID


def is_valid_created_at(created_at_value) -> bool:
    """Verifica si el valor de created_at es válido."""
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False


def normalize_datetime(dt):
    """Normaliza un datetime para quitar información de timezone."""
    if dt is None:
        return None
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt
