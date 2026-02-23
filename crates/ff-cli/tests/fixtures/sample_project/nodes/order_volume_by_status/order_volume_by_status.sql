select status, order_count
from (select status, count(*) as order_count from fct_orders group by status)
where order_count >= min_count
