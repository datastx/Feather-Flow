SELECT
    o.id,
    o.amount,
    c.name AS customer_name
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.id
