-- models/marts/dim_customers.sql
-- Gold dimension: one row per customer with enriched attributes.
-- Materialised as a TABLE in retailpulse_gold.

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders_summary as (
    -- Attach basic order stats so analysts don't need a join for simple queries
    select
        cast(user_id as string)                     as customer_id,
        count(distinct order_id)                    as total_orders,
        min(order_date)                             as first_order_date,
        max(order_date)                             as latest_order_date
    from {{ ref('stg_orders') }}
    group by 1
)

select
    -- Identity
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.phone,

    -- Demographics
    c.gender,
    c.date_of_birth,
    date_diff(current_date(), c.date_of_birth, year)    as age_years,

    -- Geography
    c.city,
    c.country,

    -- Lifecycle
    c.registered_at,
    date_diff(current_date(), date(c.registered_at), day) as days_since_registration,

    -- Order stats (pre-aggregated)
    coalesce(o.total_orders, 0)                     as total_orders,
    o.first_order_date,
    o.latest_order_date,

    -- Audit
    c.dbt_loaded_at

from customers as c
left join orders_summary as o using (customer_id)