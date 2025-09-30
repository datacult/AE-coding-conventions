
-- depends_on: {{ ref('identity_change_detector') }}
-- depends_on: {{ ref('identity__person_stream') }}

{{ config(
    materialized='incremental',
    unique_key=['anonymous_customer_id', 'customer']
) }}

WITH devices_to_process AS (
    {% if is_incremental() %}
        -- Only process devices with recent identity changes
        SELECT anonymous_customer_id
        FROM {{ ref('identity_change_detector') }}
        WHERE last_identity_event >= CURRENT_DATE - 1
    {% else %}
        -- Full refresh: process all devices
        SELECT DISTINCT anonymous_customer_id
        FROM {{ ref('identity__person_stream') }}
    {% endif %}
),

relevant_identity_events AS (
    SELECT *
    FROM {{ ref('identity__person_stream') }}
    WHERE anonymous_customer_id IN (SELECT anonymous_customer_id FROM devices_to_process)
),

identity_with_transitions AS (
    SELECT
        *,
        LEAD(customer) OVER (PARTITION BY anonymous_customer_id ORDER BY ts) AS next_customer
    FROM relevant_identity_events
    WHERE identity_sequence = 1  -- Only first occurrence of each identity
),

change_points AS (
    SELECT *
    FROM identity_with_transitions  
    WHERE next_customer IS NULL OR customer != next_customer
),

time_windows AS (
    SELECT
        anonymous_customer_id,
        customer,
        ts::timestamp_ntz as identity_timestamp,
        CASE 
            WHEN LAG(ts) OVER (PARTITION BY anonymous_customer_id ORDER BY ts) IS NULL 
            THEN '1970-01-01'::timestamp
            ELSE DATEADD('minute', -30, ts::timestamp_ntz)
        END AS window_start,
        COALESCE(
            LEAD(DATEADD('minute', -30, ts::timestamp_ntz)) OVER (PARTITION BY anonymous_customer_id ORDER BY ts),
            '2200-01-01'::timestamp_ntz
        ) AS window_end
    FROM change_points
)

SELECT * FROM time_windows