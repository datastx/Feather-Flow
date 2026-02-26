SELECT date_trunc('month', ts) AS trunc_ts, date_part('year', ts) AS yr FROM raw_data
