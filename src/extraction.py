"""
extraction.py — Módulo de extracción (scraping).

Contiene toda la lógica de scraping del sitio de la ANI.
La lógica de extracción se mantiene intacta respecto al código original.
"""

import logging
from datetime import datetime
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

from utils import (
    ENTITY_VALUE,
    FIXED_CLASSIFICATION_ID,
    URL_BASE,
    clean_quotes,
    get_rtype_id,
    is_valid_created_at,
)

logger = logging.getLogger("ani_scraping.extraction")


# ---------------------------------------------------------------------------
# Funciones de extracción por campo
# ---------------------------------------------------------------------------
def extract_title_and_link(row, norma_data: dict, verbose: bool, row_num: int) -> bool:
    """
    Extrae título y enlace de una fila.

    Returns:
        True si se extrajo correctamente, False si debe saltarse.
    """
    title_cell = row.find("td", class_="views-field views-field-title")
    if not title_cell:
        if verbose:
            logger.debug("No se encontró celda de título en la fila %d. Saltando.", row_num)
        return False

    title_link = title_cell.find("a")
    if not title_link:
        if verbose:
            logger.debug("No se encontró enlace en la fila %d. Saltando.", row_num)
        return False

    # Procesar título
    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)

    # Validar longitud del título
    if len(cleaned_title) > 65:
        if verbose:
            logger.debug(
                "Saltando norma con título demasiado largo: '%s' (longitud: %d)",
                cleaned_title, len(cleaned_title),
            )
        return False

    norma_data["title"] = cleaned_title

    # Procesar enlace
    external_link = title_link.get("href")
    if external_link and not external_link.startswith("http"):
        external_link = "https://www.ani.gov.co" + external_link

    norma_data["external_link"] = external_link
    norma_data["gtype"] = "link" if external_link else None

    # Validar que tenga enlace
    if not norma_data["external_link"]:
        if verbose:
            logger.debug(
                "Saltando norma '%s' por no tener enlace externo válido.",
                norma_data["title"],
            )
        return False

    return True


def extract_summary(row, norma_data: dict) -> None:
    """Extrae el resumen/descripción de una fila."""
    summary_cell = row.find("td", class_="views-field views-field-body")
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        formatted_summary = cleaned_summary.capitalize()
        norma_data["summary"] = formatted_summary
    else:
        norma_data["summary"] = None


def extract_creation_date(row, norma_data: dict, verbose: bool, row_num: int) -> bool:
    """
    Extrae la fecha de creación de una fila.

    Returns:
        True si se extrajo correctamente, False si debe saltarse.
    """
    fecha_cell = row.find("td", class_="views-field views-field-field-fecha--1")
    if fecha_cell:
        fecha_span = fecha_cell.find("span", class_="date-display-single")
        if fecha_span:
            created_at_raw = fecha_span.get("content", fecha_span.get_text(strip=True))
            # Procesar diferentes formatos de fecha
            if "T" in created_at_raw:
                norma_data["created_at"] = created_at_raw.split("T")[0]
            elif "/" in created_at_raw:
                try:
                    day, month, year = created_at_raw.split("/")
                    norma_data["created_at"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    norma_data["created_at"] = created_at_raw
            else:
                norma_data["created_at"] = created_at_raw
        else:
            norma_data["created_at"] = fecha_cell.get_text(strip=True)
    else:
        norma_data["created_at"] = None

    # Validar fecha
    if not is_valid_created_at(norma_data["created_at"]):
        if verbose:
            logger.debug(
                "Saltando norma '%s' por no tener fecha de creación válida (created_at: %s).",
                norma_data.get("title", "?"), norma_data["created_at"],
            )
        return False

    return True


# ---------------------------------------------------------------------------
# Scraping de una página
# ---------------------------------------------------------------------------
def scrape_page(page_num: int, verbose: bool = False) -> List[Dict]:
    """
    Scrapea una página específica del sitio de la ANI.

    Args:
        page_num: Número de página a scrapear.
        verbose: Si mostrar logs detallados.

    Returns:
        Lista de diccionarios con los datos extraídos.
    """
    # Construir URL de la página
    if page_num == 0:
        page_url = URL_BASE
    else:
        page_url = f"{URL_BASE}&page={page_num}"

    if verbose:
        logger.info("Scrapeando página %d: %s", page_num, page_url)

    try:
        # Realizar solicitud HTTP
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()

        # Parsear HTML
        soup = BeautifulSoup(response.content, "html.parser")
        tbody = soup.find("tbody")

        if not tbody:
            if verbose:
                logger.info("No se encontró tabla en página %d", page_num)
            return []

        rows = tbody.find_all("tr")
        if verbose:
            logger.info("Encontradas %d filas en página %d", len(rows), page_num)

        # Procesar filas
        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                # Estructura base del registro
                norma_data = {
                    "created_at": None,
                    "update_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "is_active": True,
                    "title": None,
                    "gtype": None,
                    "entity": ENTITY_VALUE,
                    "external_link": None,
                    "rtype_id": None,
                    "summary": None,
                    "classification_id": FIXED_CLASSIFICATION_ID,
                }

                # Extraer datos
                if not extract_title_and_link(row, norma_data, verbose, i):
                    continue

                extract_summary(row, norma_data)

                if not extract_creation_date(row, norma_data, verbose, i):
                    continue

                # Establecer rtype_id basado en título
                norma_data["rtype_id"] = get_rtype_id(norma_data["title"])

                page_data.append(norma_data)

            except Exception as e:
                if verbose:
                    logger.warning(
                        "Error procesando fila %d en página %d: %s", i, page_num, e
                    )
                continue

        return page_data

    except requests.RequestException as e:
        logger.error("Error HTTP en página %d: %s", page_num, e)
        return []
    except Exception as e:
        logger.error("Error procesando página %d: %s", page_num, e)
        return []


# ---------------------------------------------------------------------------
# Punto de entrada del módulo
# ---------------------------------------------------------------------------
def run_extraction(num_pages: int = 9) -> List[Dict]:
    """
    Ejecuta la extracción completa de *num_pages* páginas.

    Returns:
        Lista de diccionarios con todos los registros extraídos.
    """
    logger.info("Iniciando extracción — páginas a procesar: %d", num_pages)

    all_records = []  # type: List[Dict]

    for page_num in range(num_pages):
        logger.info("Procesando página %d/%d …", page_num + 1, num_pages)
        page_data = scrape_page(page_num, verbose=True)
        all_records.extend(page_data)

        # Indicador de progreso cada 3 páginas
        if (page_num + 1) % 3 == 0:
            logger.info(
                "Procesadas %d/%d páginas. Registros válidos hasta ahora: %d",
                page_num + 1, num_pages, len(all_records),
            )

    logger.info("Extracción finalizada — total registros extraídos: %d", len(all_records))
    return all_records
