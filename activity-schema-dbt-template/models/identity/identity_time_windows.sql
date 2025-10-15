-- depends_on: {{ ref('identity_change_detector') }}
-- depends_on: {{ ref('identity__person_stream') }}

{{ config(
    materialized='incremental',
    unique_key=['anonymous_customer_id', 'customer']
) }}

with devices_to_process AS (
    {% if is_incremental() %}
        -- Only process devices with recent identity changes
        select anonymous_customer_id
        from {{ ref('identity_change_detector') }}
        where last_identity_event >= current_date - 7 
    {% else %}
        -- Full refresh: process all devices
        select distinct anonymous_customer_id
        from {{ ref('identity__person_stream') }}
    {% endif %}
),

relevant_identity_events AS (
    select *
    from {{ ref('identity__person_stream') }}
    where anonymous_customer_id in (select anonymous_customer_id from devices_to_process)
),

identity_with_transitions as (
    select
        *,
        lead(customer) over (partition by anonymous_customer_id order by ts) as next_customer
    from relevant_identity_events
    where identity_sequence = 1  -- Only first occurrence of each identity
),

change_points as (
    select *
    from identity_with_transitions  
    where next_customer is null or customer != next_customer
),

time_windows as (
    select
        anonymous_customer_id,
        customer,
        ts::timestamp_ntz as identity_timestamp,
        case 
            when lag(ts) over (partition by anonymous_customer_id order by ts) is null 
            then '1970-01-01'::timestamp
            else dateadd('minute', -30, ts::timestamp_ntz)
        end as window_start,
        coalesce(
            lead(dateadd('minute', -30, ts::timestamp_ntz)) over (partition by anonymous_customer_id order by ts),
            '2200-01-01'::timestamp_ntz
        ) as window_end
    from change_points
)

select * from time_windows