-- models/marts/dim_products.sql
-- Gold dimension: one row per product enriched with sales performance stats.
-- Materialised as a TABLE in retailpulse_gold.

with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select
        product_id,
        count(distinct order_id)                    as orders_containing_product,
        sum(quantity)                               as total_units_sold,
        round(sum(discounted_line_total), 2)        as total_revenue,
        round(avg(unit_price), 2)                   as avg_selling_price,
        round(avg(discount_pct), 2)                 as avg_discount_pct
    from {{ ref('int_order_items') }}
    group by 1
)

select
    -- Identity
    p.product_id,
    p.title,
    p.category,
    p.description,
    p.image_url,
    p.price                                         as list_price,

    -- Sales performance
    coalesce(oi.orders_containing_product, 0)       as orders_containing_product,
    coalesce(oi.total_units_sold, 0)                as total_units_sold,
    coalesce(oi.total_revenue, 0.0)                 as total_revenue,
    oi.avg_selling_price,
    oi.avg_discount_pct,

    -- Audit
    p.dbt_loaded_at

from products as p
left join order_items as oi using (product_id)
