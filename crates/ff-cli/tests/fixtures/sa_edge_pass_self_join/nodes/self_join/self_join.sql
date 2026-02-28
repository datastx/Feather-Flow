select
    t1.id
    ,
    t2.name as parent_name
from raw_data t1
join raw_data t2 on t1.parent_id = t2.id
