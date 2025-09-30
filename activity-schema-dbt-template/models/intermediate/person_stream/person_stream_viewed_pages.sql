-- REQUIRED: Add dependency hint for incremental logic
-- depends_on: {{ ref('identity_change_detector') }}

{{ config(
    materialized='incremental',
    unique_key='activity_id',
    merge_update_columns=['customer']
) }}

{% call resolve_identity() %}

with arcee_customers as (
    select customer
    from {{ ref('stg_zitadel__events') }}
    where
        event_type in ('user.human.added', 'user.human.selfregistered')
        and email like '%@arcee%'
)

select
    activity_id,
    anonymous_customer_id,
    customer,
    activity,
    ts,
    revenue_impact,
    link,
    object_construct(
        'path', feature_path
    ) as feature_json
from {{ ref('stg_posthog__events') }}
where
    event = '$pageview'
    and not regexp_like(
        host,
        '.*(arcee.*dev|dev.*arcee|localhost|127\.0\.0\.1).*',
        'i'
    )
    and anonymous_customer_id not in (
        select arcee_customers.customer
        from arcee_customers
    )

    -- COPY AS-IS: Standard incremental logic
    {% if is_incremental() %}
        and (
            ts > (select max(ts) from {{ this }})
            or anonymous_customer_id in (             
            -- MODIFY: replace user_distinct_id with anonymous_customer_id 
		            --or any respective field
                select anonymous_customer_id 
                from {{ ref('identity_change_detector') }}
                where last_identity_event >= current_date - 1
            )
        )
    {% endif %}


{% endcall %}