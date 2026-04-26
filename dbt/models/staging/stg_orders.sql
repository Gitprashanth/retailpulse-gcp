-- models/staging/stg_orders.sql
-- Cleans and type-casts raw_orders from the Bronze layer.
-- Products are kept as a JSON STRING for downstream unnesting in int_order_items.

with source as (
    select * from {{ source('bronze', 'raw_orders') }}
),

staged as (
    select
        -- Keys
        cast(order_id   as int64)       as order_id,
        cast(user_id    as int64)       as user_id,

        -- Dates
        cast(date as date)                  as order_date,

        -- Status — normalise to lowercase
        lower(trim(status))             as status,

        -- Products kept as raw JSON string; unnested in int_order_items
        products,

        -- Audit
        current_timestamp()             as dbt_loaded_at

    from source
    where order_id is not null
      and user_id  is not null
)

select * from staged