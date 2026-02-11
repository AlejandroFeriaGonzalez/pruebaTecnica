-- =============================================================================
-- init.sql — DDL de tablas para el scraping de normativas ANI
-- =============================================================================
-- Tablas:
--   1. regulations          → datos principales de normativas
--   2. regulations_component → relación norma–componente
-- =============================================================================

-- ─────────────────────────────────────────────────
-- Tabla principal: regulations
-- ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS regulations (
    id              SERIAL PRIMARY KEY,
    created_at      DATE,                           -- Fecha del documento ('YYYY-MM-DD')
    update_at       TIMESTAMP,                      -- Última actualización
    is_active       BOOLEAN DEFAULT TRUE,           -- Registro activo
    title           VARCHAR(100) NOT NULL,          -- Título (máx. 65 chars según validación)
    gtype           VARCHAR(20),                    -- Tipo de recurso ('link' o NULL)
    entity          VARCHAR(255) NOT NULL,          -- Entidad emisora
    external_link   TEXT,                           -- URL del documento
    rtype_id        INTEGER,                        -- 14=decreto, 15=resolución
    summary         TEXT,                           -- Resumen
    classification_id INTEGER                       -- Clasificación (fijo 13)
);

-- Índices para consultas de duplicados y búsqueda por entidad
CREATE INDEX IF NOT EXISTS idx_regulations_entity
    ON regulations (entity);

CREATE INDEX IF NOT EXISTS idx_regulations_entity_created
    ON regulations (entity, created_at DESC);

-- Restricción UNIQUE para idempotencia a nivel de BD
-- Previene duplicados incluso si la validación en Python falla
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_regulations_title_date_link'
    ) THEN
        ALTER TABLE regulations
            ADD CONSTRAINT uq_regulations_title_date_link
            UNIQUE (title, created_at, external_link);
    END IF;
END $$;

-- ─────────────────────────────────────────────────
-- Tabla de relación: regulations_component
-- ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS regulations_component (
    id               SERIAL PRIMARY KEY,
    regulations_id   INTEGER NOT NULL REFERENCES regulations(id) ON DELETE CASCADE,
    components_id    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_regcomp_regulation
    ON regulations_component (regulations_id);

-- ─────────────────────────────────────────────────
-- Vista de compatibilidad
-- ─────────────────────────────────────────────────
CREATE OR REPLACE VIEW dapper_regulations_regulations AS
    SELECT * FROM regulations;
