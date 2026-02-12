SELECT
    t1.id,
    t2.name AS parent_name
FROM raw_data t1
JOIN raw_data t2 ON t1.parent_id = t2.id
