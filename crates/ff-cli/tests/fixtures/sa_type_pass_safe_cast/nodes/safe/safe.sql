select
    cast(id as bigint) as id_big
    ,
    cast(id as float) as id_float
    ,
    cast(d as timestamp) as d_ts
from raw_data
