with unioned_activities as (
    select
        activity,
        anonymous_customer_id,
        customer,
        activity_id,
        ts

    from {{ ref('stg__payment_data') }}       -- MODIFY: Your staging table
    where
        anonymous_customer_id is not null
        and customer is not null

    union all

    select
        activity,
        anonymous_customer_id,
        customer,
        activity_id,
        ts
    from {{ ref('stg__event_data') }}         
    where activity = 'identify' 
        and anonymous_customer_id is not null
        and customer is not null

    union all

    select
        activity,
        anonymous_customer_id,
        customer,
        activity_id,
        ts
    from {{ ref('stg__signed_up_data') }}     -- MODIFY: Your staging table
    where
        anonymous_customer_id is not null
        and customer is not null

)
select
    *,
    row_number() over (
        partition by anonymous_customer_id, customer
        order by ts
    ) as identity_sequence
    -- {{ dbt_utils.generate_surrogate_key(['activity_id', 'anonymous_customer_id']) }} as unique_id
from unioned_activities
