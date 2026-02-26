SELECT
    o.id
FROM stg_orders o
JOIN stg_items i ON o.code = i.order_code
