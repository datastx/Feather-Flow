SELECT
    a.id
FROM source_a a
JOIN source_b b ON a.id = b.id AND a.val > b.val
