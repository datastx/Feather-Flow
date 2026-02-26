SELECT
    CAST(id AS BIGINT) AS id_big,
    CAST(id AS FLOAT) AS id_float,
    CAST(d AS TIMESTAMP) AS d_ts
FROM raw_data
