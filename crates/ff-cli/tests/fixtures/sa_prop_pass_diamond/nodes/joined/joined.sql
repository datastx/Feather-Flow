select
    a.id
    , a.a
    , b.b
from branch_a a
join branch_b b on a.id = b.id
