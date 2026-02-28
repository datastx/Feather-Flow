select
    regexp_matches(
        name
        , '^A'
    ) as matches
    , md5(name) as hash
from raw_data
