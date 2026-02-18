-- ============================================================
-- v002: Add effective_classification for lineage-based propagation
-- ============================================================
-- Adds effective_classification to model_columns, which holds the
-- computed classification after propagating through column lineage.
-- The existing `classification` column remains the declared (YAML) value.
-- Application-level validation ensures values are in
-- ('pii', 'sensitive', 'internal', 'public').

ALTER TABLE ff_meta.model_columns
    ADD COLUMN effective_classification VARCHAR;

-- Recreate v_columns to include effective_classification
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
    mc.is_primary_key,
    mc.classification,
    mc.effective_classification,
    mc.ordinal_position
FROM ff_meta.model_columns mc
JOIN ff_meta.models m ON mc.model_id = m.model_id;

-- Recreate v_lineage to include effective_classification
DROP VIEW IF EXISTS ff_meta.v_lineage;
CREATE VIEW ff_meta.v_lineage AS
SELECT
    cl.lineage_id,
    tgt.name AS target_model,
    cl.target_column,
    COALESCE(src.name, cl.source_table) AS source_model,
    cl.source_column,
    cl.lineage_kind,
    cl.is_direct,
    tgt_col.classification AS target_classification,
    src_col.classification AS source_classification,
    tgt_col.effective_classification AS target_effective_classification,
    src_col.effective_classification AS source_effective_classification
FROM ff_meta.column_lineage cl
JOIN ff_meta.models tgt ON cl.target_model_id = tgt.model_id
LEFT JOIN ff_meta.models src ON cl.source_model_id = src.model_id
LEFT JOIN ff_meta.model_columns tgt_col
    ON tgt_col.model_id = cl.target_model_id AND tgt_col.name = cl.target_column
LEFT JOIN ff_meta.model_columns src_col
    ON src_col.model_id = cl.source_model_id AND src_col.name = cl.source_column;
