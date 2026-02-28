select
    order_id
    , customer_id
    , order_date
    , amount
    , status
from stg_orders
{% if is_exists() %}
    where order_date > (select max(order_date) from fct_orders_is_exists)
{% endif %}
