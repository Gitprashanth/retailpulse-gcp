-- models/intermediate/int_order_items.sql
-- Unnests the products JSON array from stg_orders into one row per order line item.
-- Joins with stg_products to enrich with title, category, and price.
-- Used by: fct_orders, mart_revenue_by_category

with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

-- BigQuery: JSON_QUERY_ARRAY parses the products string into a ARRAY<JSON>
unnested as (
    select
        o.order_id,
        o.user_id,
        o.order_date,
        o.status,

        -- Each element in the products array
        cast(json_value(item, '$.id')       as int64)   as product_id,
        cast(json_value(item, '$.quantity') as int64)   as quantity,
        safe_cast(json_value(item, '$.price')    as float64) as unit_price,
        safe_cast(json_value(item, '$.total')    as float64) as line_total,
        safe_cast(
            json_value(item, '$.discountPercentage') as float64
        )                                               as discount_pct

    from orders as o,
    -- unnest(json_query_array(o.products)) as item
    unnest(json_query_array(json_value(o.products))) as item
),

-- Enrich with product-level attributes
enriched as (
    select
        u.order_id,
        u.user_id,
        u.order_date,
        u.status,
        u.product_id,
        p.title                         as product_title,
        p.category                      as product_category,
        u.quantity,
        u.unit_price,
        u.discount_pct,
        u.line_total,

        -- Recalculate discounted total for safety
        round(
            u.unit_price * u.quantity * (1 - coalesce(u.discount_pct, 0) / 100),
            2
        )                               as discounted_line_total

    from unnested as u
    left join products as p using (product_id)
)

select * from enriched
