-- This model creates a circular dependency with circular_a
-- circular_b -> circular_a -> circular_b

SELECT
    id,
    name,
    total_value
FROM circular_a
WHERE status = 'completed'
