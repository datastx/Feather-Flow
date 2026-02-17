-- rule: model_has_description
-- severity: warn
-- description: Every model should have a description in its YAML schema
SELECT
    name AS model_name,
    'Model is missing a description' AS violation
FROM ff_meta.models
WHERE description IS NULL
   OR description = ''
