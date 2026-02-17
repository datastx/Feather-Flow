-- rule: model_has_primary_key
-- severity: warn
-- description: Every model should have at least one primary_key constraint
SELECT
    m.name AS model_name,
    'No primary key constraint defined' AS violation
FROM ff_meta.models m
WHERE NOT EXISTS (
    SELECT 1
    FROM ff_meta.model_column_constraints mcc
    JOIN ff_meta.model_columns mc ON mc.column_id = mcc.column_id
    WHERE mc.model_id = m.model_id
      AND mcc.constraint_type = 'primary_key'
)
