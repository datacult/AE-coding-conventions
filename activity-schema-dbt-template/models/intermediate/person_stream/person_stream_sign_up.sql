select
    activity_id,
    anonymous_customer_id,
    customer,
    activity,
    ts,
    null as revenue_impact,
    null as link,
    object_construct(
        'email', feature_email,
        'username', feature_username
    ) as feature_json,
    -- ADD THIS TO EVERY person_stream_* MODEL (AS-IS)
    row_number() over (
        partition by 
            activity,  -- Same activity type
            coalesce(customer, anonymous_customer_id)  -- Same user
        order by ts asc
    ) as activity_occurrence,

    lead(ts) over (
        partition by 
            activity, 
            coalesce(customer, anonymous_customer_id)
        order by ts asc
    ) as activity_repeated_at
from {{ ref('stg_zitadel__events') }}
where event_type in ('user.human.added', 'user.human.selfregistered')
