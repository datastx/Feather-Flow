SELECT
    a.id AS a_id,
    b.id AS b_id
FROM source_a a
CROSS JOIN source_b b
