select
    a.id
    , b.name
from raw_data a
join raw_data b on a.id = b.id
