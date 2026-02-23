{{ config(materialized="table", schema="analytics") }}

select
    product_id,
    product_name,
    category,
    price,
    case
        when category = 'electronics'
        then 'high_value'
        when category = 'tools'
        then 'medium_value'
        else 'standard'
    end as category_group,
    case
        when price > 100 then 'premium' when price > 25 then 'standard' else 'budget'
    end as price_tier
from stg_products
where active = true
