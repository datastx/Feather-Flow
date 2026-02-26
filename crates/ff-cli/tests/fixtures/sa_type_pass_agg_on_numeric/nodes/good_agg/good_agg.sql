SELECT
    SUM(amount) AS total_amount,
    COUNT(name) AS name_count,
    MIN(name) AS first_name,
    MAX(name) AS last_name
FROM raw_data
