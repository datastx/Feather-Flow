select
    id
    ,
    cents_to_dollars(amount_cents) as amount_dollars
from raw_orders
