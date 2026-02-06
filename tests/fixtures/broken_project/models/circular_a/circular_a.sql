-- This model creates a circular dependency with circular_b
-- circular_a -> circular_b -> circular_a

SELECT
    id,
    name,
    value
FROM circular_b
WHERE active = true
