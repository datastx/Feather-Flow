-- ============================================================
-- v003: Add description_ai_generated for AI provenance tracking
-- ============================================================
-- Adds description_ai_generated to all tables that have a description
-- column. This is a tri-state boolean:
--   true  = AI-generated description
--   false = human-written description
--   NULL  = unknown provenance
-- Defaults to NULL (unknown) for existing rows.

ALTER TABLE ff_meta.models
    ADD COLUMN description_ai_generated BOOLEAN;

ALTER TABLE ff_meta.model_columns
    ADD COLUMN description_ai_generated BOOLEAN;

ALTER TABLE ff_meta.sources
    ADD COLUMN description_ai_generated BOOLEAN;

ALTER TABLE ff_meta.source_tables
    ADD COLUMN description_ai_generated BOOLEAN;

ALTER TABLE ff_meta.source_columns
    ADD COLUMN description_ai_generated BOOLEAN;

-- Recreate v_columns to include description_ai_generated
DROP VIEW IF EXISTS ff_meta.v_columns;
CREATE VIEW ff_meta.v_columns AS
SELECT
    mc.column_id,
    m.name AS model_name,
    mc.name AS column_name,
    mc.declared_type,
    mc.inferred_type,
    mc.nullability_declared,
    mc.nullability_inferred,
    mc.description,
    mc.description_ai_generated,
    mc.is_primary_key,
    mc.classification,
    mc.effective_classification,
    mc.ordinal_position
FROM ff_meta.model_columns mc
JOIN ff_meta.models m ON mc.model_id = m.model_id;

-- Recreate v_source_columns to include description_ai_generated
DROP VIEW IF EXISTS ff_meta.v_source_columns;
CREATE VIEW ff_meta.v_source_columns AS
SELECT
    s.name AS source_name,
    s.database_name,
    s.schema_name,
    st.name AS table_name,
    st.identifier AS actual_table_name,
    sc.name AS column_name,
    sc.data_type,
    sc.description,
    sc.description_ai_generated
FROM ff_meta.source_columns sc
JOIN ff_meta.source_tables st ON sc.source_table_id = st.source_table_id
JOIN ff_meta.sources s ON st.source_id = s.source_id;
