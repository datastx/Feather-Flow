SELECT
    o.amount,
    COALESCE(c.name, 'Unknown') AS name,
    COALESCE(c.email, 'none@example.com') AS email
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.id
