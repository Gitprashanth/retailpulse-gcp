-- models/marts/fct_orders.sql
-- Gold fact table: one row per order with aggregated financials.
-- Line-item detail lives in int_order_items (Silver).
-- Materialised as a TABLE in retailpulse_gold.

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        count(*)                                    as total_line_items,
        sum(quantity)                               as total_quantity,
        round(sum(line_total), 2)                   as gross_order_total,
        round(sum(discounted_line_total), 2)        as net_order_total,
        round(sum(line_total) - sum(discounted_line_total), 2) as total_discount_amount
    from {{ ref('int_order_items') }}
    group by 1
),

customers as (
    select customer_id, full_name, city, country
    from {{ ref('dim_customers') }}
)

select
    -- Keys
    o.order_id,
    o.user_id                                       as customer_id,
    cast(o.user_id as string)                       as customer_id_str,   -- for join safety

    -- Dates
    o.order_date,
    extract(year  from o.order_date)                as order_year,
    extract(month from o.order_date)                as order_month,
    extract(dayofweek from o.order_date)            as order_day_of_week,

    -- Status
    o.status,
    case
        when o.status in ('delivered', 'shipped')   then 'completed'
        when o.status in ('cancelled', 'returned')  then 'failed'
        else 'in_progress'
    end                                             as status_group,

    -- Financials (from aggregated line items)
    oi.total_line_items,
    oi.total_quantity,
    oi.gross_order_total,
    oi.net_order_total,
    oi.total_discount_amount,

    -- Customer enrichment
    c.full_name                                     as customer_name,
    c.city                                          as customer_city,
    c.country                                       as customer_country

from orders as o
left join order_items as oi using (order_id)
left join customers   as c on cast(o.user_id as string) = c.customer_id
