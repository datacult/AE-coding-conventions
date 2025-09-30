with unioned_activities as (
    select
        activity,
        anonymous_customer_id,
        customer,
        activity_id,
        ts

    from {{ ref('stg_stripe__arcee_customers') }}
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
    from {{ ref('stg_stripe__customers') }}
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
    from {{ ref('stg_posthog__identify') }}
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
    from {{ ref('stg_lago__lago' ) }}
    where
        activity = 'purchased_credits'
        and anonymous_customer_id is not null
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
