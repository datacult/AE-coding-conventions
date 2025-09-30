with unioned_activities as (
    select
        activity_id,
        anonymous_customer_id,
        customer,
        activity,
        ts,
        null as revenue_impact, 
        null as link, 
        null as feature_json

    from {{ ref('stg_stripe__arcee_customers') }}
    where anonymous_customer_id is not null 
    and customer is not null 

    union all

    select
        activity_id,
        anonymous_customer_id,
        customer,
        activity,
        ts,
        null as revenue_impact, 
        null as link, 
        null as feature_json

    from {{ ref('stg_stripe__customers') }}
    where anonymous_customer_id is not null 
    and customer is not null 

    union all

    select
        activity_id,
        anonymous_customer_id,
        customer,
        activity,
        ts,
        null as revenue_impact, 
        null as link, 
        null as feature_json

    from {{ ref('stg_posthog__identify') }}
    where anonymous_customer_id is not null 
    and customer is not null 

    union all

    select
        activity_id,
        anonymous_customer_id,
        customer,
        activity,
        ts,
        null as revenue_impact, 
        null as link, 
        null as feature_json

    from {{ ref('person_stream_purchased_credits') }}
    where anonymous_customer_id is not null 
    and customer is not null 
)

select
    *,
    ROW_NUMBER() OVER (
        PARTITION BY anonymous_customer_id, customer 
        ORDER BY ts
    ) as identity_sequence
    -- {{ dbt_utils.generate_surrogate_key(['activity_id', 'anonymous_customer_id']) }} as unique_id
from unioned_activities
