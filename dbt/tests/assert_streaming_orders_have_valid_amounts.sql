-- Validates Phase 6 streaming data in Bronze.
-- Every streaming order must have a positive amount and non-null IDs.
-- Returns bad rows — test passes when this returns 0 rows.

select *
from `retailpulse-gcp.retailpulse_bronze.streaming_orders`
where
    amount is null
    or amount <= 0
    or customer_id is null
    or product_id is null