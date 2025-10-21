{{ config(
    materialized='incremental',
    unique_key='anonymous_customer_id'
) }}

select 
    anonymous_customer_id,
    max(ts) as last_identity_event,
    count(distinct customer) as customer_count,
    listagg(distinct customer, ', ') as customers,
    count(*) as total_identity_events
from {{ ref('identity__person_stream') }}

{% if is_incremental() %}
    where ts > (select coalesce(max(last_identity_event), '1900-01-01') from {{ this }})
{% endif %}

group by anonymous_customer_id