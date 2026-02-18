SELECT
    id AS order_id,
    user_id AS customer_id,
    amount,
    status
FROM raw_orders
