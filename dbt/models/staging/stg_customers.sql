-- models/staging/stg_customers.sql
-- Cleans and type-casts raw_customers from the Bronze layer.

with source as (
    select * from {{ source('bronze', 'raw_customers') }}
),

staged as (
    select
        -- Keys
        cast(customer_id as string)             as customer_id,

        -- Name
        initcap(trim(first_name))               as first_name,
        initcap(trim(last_name))                as last_name,
        concat(
            initcap(trim(first_name)), ' ',
            initcap(trim(last_name))
        )                                       as full_name,

        -- Contact
        lower(trim(email))                      as email,
        trim(phone)                             as phone,

        -- Demographics
        lower(trim(gender))                     as gender,
        cast(date_of_birth as date)                     as date_of_birth,

        -- Geography
        initcap(trim(city))                     as city,
        initcap(trim(country))                  as country,

        -- Registration
        cast(registered_at as timestamp)                as registered_at,

        -- Audit
        current_timestamp()                     as dbt_loaded_at

    from source
    where customer_id is not null
      and email is not null
),

-- Deduplicate on customer_id
deduped as (
    select *
    from (
        select *,
            row_number() over (partition by customer_id order by registered_at desc) as rn
        from staged
    )
    where rn = 1
)

select
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    gender,
    date_of_birth,
    city,
    country,
    registered_at,
    dbt_loaded_at
from deduped