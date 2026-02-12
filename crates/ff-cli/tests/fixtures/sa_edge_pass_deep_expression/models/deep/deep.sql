SELECT
    CAST(COALESCE(CASE WHEN amount > 0 THEN amount ELSE 0 END, 0) AS BIGINT) AS safe_amount
FROM raw_data
