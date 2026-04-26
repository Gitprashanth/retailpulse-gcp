-- models/staging/stg_products.sql
-- Cleans and type-casts raw_products from the Bronze layer.

with source as (
    select * from {{ source('bronze', 'raw_products') }}
),

staged as (
    select
        -- Keys
        cast(product_id as int64)       as product_id,

        -- Attributes
        trim(title)                     as title,
        lower(trim(category))           as category,
        trim(description)               as description,
        image                           as image_url,

        -- Price — guard against nulls / bad data
        safe_cast(price as float64)     as price,

        -- Audit
        current_timestamp()             as dbt_loaded_at

    from source
    where product_id is not null
),

-- Deduplicate on product_id, keep the latest record
deduped as (
    select *
    from (
        select *,
            row_number() over (partition by product_id order by dbt_loaded_at desc) as rn
        from staged
    )
    where rn = 1
)

select
    product_id,
    title,
    category,
    description,
    image_url,
    price,
    dbt_loaded_at
from deduped
