-- models/marts/mart_revenue_by_category.sql
-- Gold summary mart: revenue, volume, and order metrics rolled up by product category.
-- Sliced by month for trend analysis in Looker Studio.
-- Materialised as a TABLE in retailpulse_gold.

with order_items as (
    select * from {{ ref('int_order_items') }}
),

monthly_category as (
    select
        -- Time grain: month
        date_trunc(order_date, month)               as month_start,
        extract(year  from order_date)              as year,
        extract(month from order_date)              as month,

        -- Category
        coalesce(product_category, 'unknown')       as product_category,

        -- Volume
        count(distinct order_id)                    as distinct_orders,
        sum(quantity)                               as units_sold,
        count(distinct user_id)                     as distinct_customers,

        -- Revenue
        round(sum(line_total), 2)                   as gross_revenue,
        round(sum(discounted_line_total), 2)        as net_revenue,
        round(sum(line_total) - sum(discounted_line_total), 2) as total_discount,

        -- Averages
        round(avg(unit_price), 2)                   as avg_unit_price,
        round(avg(discount_pct), 2)                 as avg_discount_pct,
        round(sum(discounted_line_total) / nullif(sum(quantity), 0), 2) as revenue_per_unit

    from order_items
    group by 1, 2, 3, 4
)

select
    month_start,
    year,
    month,
    product_category,
    distinct_orders,
    units_sold,
    distinct_customers,
    gross_revenue,
    net_revenue,
    total_discount,
    avg_unit_price,
    avg_discount_pct,
    revenue_per_unit,

    -- Share of monthly total (window function)
    round(
        net_revenue / nullif(
            sum(net_revenue) over (partition by month_start),
            0
        ) * 100,
        2
    )                                               as pct_of_monthly_revenue

from monthly_category
order by month_start desc, net_revenue desc
