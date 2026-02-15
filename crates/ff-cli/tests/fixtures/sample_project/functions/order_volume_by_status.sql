SELECT status, order_count
FROM (
    SELECT status, COUNT(*) AS order_count
    FROM fct_orders
    GROUP BY status
)
WHERE order_count >= min_count
