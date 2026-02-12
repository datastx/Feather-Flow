SELECT regexp_matches(name, '^A') AS matches, md5(name) AS hash FROM raw_data
