SELECT
    o.id,
    o.amount,
    o.status,
    c.name AS customer_name,
    o.created_at
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.id
