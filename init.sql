-- =============================================================================
-- init.sql - Definición de tablas para el proyecto de scraping de normativas ANI
-- =============================================================================
-- Tablas inferidas a partir del código en lambda.py:
--   1. regulations          → insert_new_records() línea 377 y norma_data (líneas 314-325)
--   2. regulations_component → insert_regulations_component() línea 366
--
-- NOTA: En check_for_new_content() (línea 568) se usa el nombre 
--       "dapper_regulations_regulations". Para mantener coherencia con los INSERTs,
--       se crea la tabla como "regulations" y se agrega un alias (vista) para 
--       compatibilidad con las consultas de lectura.
-- =============================================================================

-- ─────────────────────────────────────────────────
-- Tabla principal: regulations
-- ─────────────────────────────────────────────────
-- Columnas inferidas del diccionario norma_data (líneas 314-325 de lambda.py):
--   created_at, update_at, is_active, title, gtype, entity,
--   external_link, rtype_id, summary, classification_id
-- Además, el SELECT de línea 502 confirma la existencia de la columna "id".

CREATE TABLE IF NOT EXISTS regulations (
    id              SERIAL PRIMARY KEY,
    created_at      DATE,                           -- Fecha de creación del documento (formato 'YYYY-MM-DD')
    update_at       TIMESTAMP,                      -- Fecha/hora de última actualización (formato 'YYYY-MM-DD HH:MI:SS')
    is_active       BOOLEAN DEFAULT TRUE,           -- Indica si el registro está activo
    title           VARCHAR(100) NOT NULL,          -- Título del documento (máximo 65 caracteres según validación, con margen)
    gtype           VARCHAR(20),                    -- Tipo de recurso ('link' o NULL)
    entity          VARCHAR(255) NOT NULL,          -- Entidad emisora (e.g. 'Agencia Nacional de Infraestructura')
    external_link   TEXT,                           -- URL del documento externo
    rtype_id        INTEGER,                        -- Tipo de regulación (14=decreto, 15=resolución)
    summary         TEXT,                           -- Resumen/descripción del documento
    classification_id INTEGER                       -- ID de clasificación (fijo: 13 para ANI)
);

-- Índice para acelerar las consultas de duplicados y búsquedas por entidad
CREATE INDEX IF NOT EXISTS idx_regulations_entity ON regulations (entity);
CREATE INDEX IF NOT EXISTS idx_regulations_entity_created ON regulations (entity, created_at DESC);

-- Restricción UNIQUE para prevenir duplicados a nivel de base de datos
-- (complementa la validación en Python en las líneas 424-441 de lambda.py)
-- ALTER TABLE regulations ADD CONSTRAINT uq_regulations_title_date_link 
--     UNIQUE (title, created_at, external_link);

-- ─────────────────────────────────────────────────
-- Tabla de relación: regulations_component
-- ─────────────────────────────────────────────────
-- Columnas inferidas de insert_regulations_component() (líneas 355-370 de lambda.py):
--   regulations_id (FK → regulations.id)
--   components_id  (fijo: 7 para ANI)

CREATE TABLE IF NOT EXISTS regulations_component (
    id               SERIAL PRIMARY KEY,
    regulations_id   INTEGER NOT NULL REFERENCES regulations(id) ON DELETE CASCADE,
    components_id    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_regcomp_regulation ON regulations_component (regulations_id);

-- ─────────────────────────────────────────────────
-- Vista de compatibilidad
-- ─────────────────────────────────────────────────
-- En check_for_new_content() (línea 568) el código consulta la tabla 
-- "dapper_regulations_regulations". Para mantener compatibilidad sin 
-- renombrar la tabla principal, se crea una vista.

CREATE OR REPLACE VIEW dapper_regulations_regulations AS
    SELECT * FROM regulations;
