-- Cross-layer consistency check.
-- net_order_total in fct_orders must match the sum of discounted_line_total
-- from int_order_items for the same order.
-- Returns rows where the two layers disagree by more than 1 cent.

with fact_totals as (
    select
        order_id,
        net_order_total
    from {{ref('fct_orders')}}
    where net_order_total is not null 
)

,item_totals as(
    select 
        order_id,
        round(sum(discounted_line_total), 2) as calculated_total
    from {{ref('int_order_items')}}
    group by order_id
)

select
    f.order_id,
    f.net_order_total,
    i.calculated_total,
    abs(f.net_order_total - i.calculated_total) as discrepancy
from fact_totals f
join item_totals i using (order_id)
where abs(f.net_order_total - i.calculated_total) > 0.01