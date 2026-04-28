-- Gold fact table must never have duplicate order_ids
-- Returns any duplicates found — test passes when this returns 0 rows.

select 
    order_id,
    count(*) as row_count
from {{ref('fct_orders')}}
group by order_id
having count(*)>1