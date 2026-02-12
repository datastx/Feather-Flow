SELECT
    o.id,
    c.name,
    c.email
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.id
