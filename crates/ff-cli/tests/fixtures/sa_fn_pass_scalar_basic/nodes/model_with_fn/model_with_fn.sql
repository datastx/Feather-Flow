SELECT
    id,
    cents_to_dollars(amount_cents) AS amount_dollars
FROM raw_orders
