{{ config(materialized="table", schema="analytics") }}

select distinct
    product_id,
    product_name,
    category,
    price,
    cast(product_id * 10 as bigint) as id_scaled,
    case
        when category = 'electronics'
        then
            case
                when price > 100 then 'premium_electronics' else 'standard_electronics'
            end
        when category = 'tools'
        then 'tools'
        else 'other'
    end as detailed_category
from stg_products
