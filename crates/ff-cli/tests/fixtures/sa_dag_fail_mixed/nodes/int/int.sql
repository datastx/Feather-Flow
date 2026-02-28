select
    s.id
    , s.name
    , s.amount
from stg s
left join stg s2 on s.id = s2.id
