# Scraper de Normatividad ANI

Este proyecto es una función AWS Lambda diseñada para automatizar la extracción de información normativa desde el portal de la **Agencia Nacional de Infraestructura (ANI)**.

## Descripción

El script (`lambda.py`) realiza web scraping del sitio oficial de la ANI para recopilar resoluciones, decretos y otras normas relevantes. Los datos extraídos se procesan, normalizan y almacenan de forma segura en una base de datos PostgreSQL.

## Características Principales

- **Scraping Automatizado**: Utiliza `BeautifulSoup` para extraer títulos, enlaces externos, resúmenes y fechas de creación de las normas.
- **Gestión de Base de Datos**: Integración con PostgreSQL mediante `psycopg2`, permitiendo el almacenamiento estructurado de la información.
- **Validación de Duplicados**: Implementa una lógica optimizada para identificar y omitir registros que ya se encuentran en la base de datos, asegurando la integridad de la información.
- **Integración con AWS**: Uso de AWS Secrets Manager para la gestión segura de credenciales en producción.
- **Entorno de Desarrollo**: Soporte para variables de entorno locales a través de `.env` para facilitar las pruebas y el desarrollo.

## Requisitos Técnicos

- **Python**: 3.x
- **Librerías principales**: `requests`, `beautifulsoup4`, `pandas`, `psycopg2`, `boto3`, `python-dotenv`.

## Ejecución Local

Para ejecutar el script en un entorno local:

```bash
uv run .\lambda.py
```

Asegúrese de contar con las variables de entorno necesarias configuradas en su archivo `.env`.
