SELECT
    status,
    COUNT(*) AS order_count
FROM fct_orders
GROUP BY status
HAVING COUNT(*) >= min_count
