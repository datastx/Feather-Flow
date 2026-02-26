SELECT
    s.id,
    s.name,
    s.amount
FROM stg s
LEFT JOIN stg s2 ON s.id = s2.id
