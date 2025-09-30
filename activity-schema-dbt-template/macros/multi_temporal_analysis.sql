{% macro multi_temporal_analysis(
    primary_cohort_activity,
    join_configs,
    cohort_filter='first',
    nth_occurrence=1
) %}

{%- set valid_join_types = [
    'first_ever', 'last_ever', 'aggregate_all_ever',
    'first_before', 'last_before', 'aggregate_before', 
    'first_after', 'last_after', 'aggregate_after',
    'first_in_between', 'last_in_between', 'aggregate_in_between'
] -%}

-- Validate all join configs
{%- for config in join_configs -%}
    {%- if config.join_type not in valid_join_types -%}
        {{ exceptions.raise_compiler_error("Invalid join_type in config '" ~ config.name ~ "': " ~ config.join_type) }}
    {%- endif -%}
{%- endfor -%}

-- Single cohort base
with cohort_base as (
    select 
        *,
        coalesce(customer, anonymous_customer_id) as join_key
    from {{ ref('person_stream_' ~ primary_cohort_activity) }}
    {% if cohort_filter == 'first' %}
    where activity_occurrence = 1
    {% elif cohort_filter == 'last' %}
    where activity_repeated_at is null
    {% elif cohort_filter == 'nth' %}
    where activity_occurrence = {{ nth_occurrence }}
    {% endif %}
),

-- For in_between joins, add next cohort timestamp
cohort_with_next as (
    select 
        *,
        lead(ts) over (partition by join_key order by ts) as next_cohort_ts
    from cohort_base
),

{% for config in join_configs %}
-- {{ config.name }}: {{ config.join_type }}
{{ config.name }}_target_stream as (
    select 
        *,
        coalesce(customer, anonymous_customer_id) as join_key
    from {{ ref('person_stream_' ~ config.target_activity) }}
),

{{ config.name }}_join as (
    {% if config.join_type == 'first_ever' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        1 as target_occurred
    from cohort_base c
    inner join (
        select 
            join_key, activity_id, ts, feature_json, activity_occurrence,
            row_number() over (partition by join_key order by ts) as rn
        from {{ config.name }}_target_stream
    ) t on c.join_key = t.join_key and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, 0
    from cohort_base c
    where not exists (select 1 from {{ config.name }}_target_stream t where c.join_key = t.join_key)
    
    {% elif config.join_type == 'last_ever' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        1 as target_occurred
    from cohort_base c
    inner join (
        select 
            join_key, activity_id, ts, feature_json, activity_occurrence,
            row_number() over (partition by join_key order by ts desc) as rn
        from {{ config.name }}_target_stream
    ) t on c.join_key = t.join_key and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, 0
    from cohort_base c
    where not exists (select 1 from {{ config.name }}_target_stream t where c.join_key = t.join_key)
    
    {% elif config.join_type == 'aggregate_all_ever' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_base c
    left join {{ config.name }}_target_stream t on c.join_key = t.join_key
    group by c.activity_id, c.join_key
    
    {% elif config.join_type == 'first_before' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', t.ts, c.ts) as days_since_target,
        1 as target_occurred
    from cohort_base c
    inner join (
        select 
            join_key, activity_id, ts, feature_json, activity_occurrence,
            row_number() over (partition by join_key order by ts) as rn
        from {{ config.name }}_target_stream
    ) t on c.join_key = t.join_key and t.ts < c.ts and t.rn = 1
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts >= dateadd('{{ config.window.split()[1] }}', -{{ config.window.split()[0] }}, c.ts)
        {% endif %}
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, null, 0
    from cohort_base c
    where not exists (
        select 1 from {{ config.name }}_target_stream t
        where c.join_key = t.join_key and t.ts < c.ts
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts >= dateadd('{{ config.window.split()[1] }}', -{{ config.window.split()[0] }}, c.ts)
        {% endif %}
    )
    
    {% elif config.join_type == 'last_before' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', t.ts, c.ts) as days_since_target,
        1 as target_occurred
    from cohort_base c
    inner join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id, t2.ts, t2.feature_json, t2.activity_occurrence,
            row_number() over (partition by c2.activity_id order by t2.ts desc) as rn
        from cohort_base c2
        inner join {{ config.name }}_target_stream t2 
            on c2.join_key = t2.join_key and t2.ts < c2.ts
            {% if config.get('window') and config.window != 'unlimited' %}
            and t2.ts >= dateadd('{{ config.window.split()[1] }}', -{{ config.window.split()[0] }}, c2.ts)
            {% endif %}
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, null, 0
    from cohort_base c
    where not exists (
        select 1 from {{ config.name }}_target_stream t
        where c.join_key = t.join_key and t.ts < c.ts
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts >= dateadd('{{ config.window.split()[1] }}', -{{ config.window.split()[0] }}, c.ts)
        {% endif %}
    )
    
    {% elif config.join_type == 'aggregate_before' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_base c
    left join {{ config.name }}_target_stream t on c.join_key = t.join_key and t.ts < c.ts
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts >= dateadd('{{ config.window.split()[1] }}', -{{ config.window.split()[0] }}, c.ts)
        {% endif %}
    group by c.activity_id, c.join_key
    
    {% elif config.join_type == 'first_after' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_to_target,
        1 as target_occurred
    from cohort_base c
    inner join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id, t2.ts, t2.feature_json, t2.activity_occurrence,
            row_number() over (partition by c2.activity_id order by t2.ts asc) as rn
        from cohort_base c2
        inner join {{ config.name }}_target_stream t2 
            on c2.join_key = t2.join_key and t2.ts > c2.ts
            {% if config.get('window') and config.window != 'unlimited' %}
            and t2.ts <= dateadd('{{ config.window.split()[1] }}', {{ config.window.split()[0] }}, c2.ts)
            {% endif %}
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, null, 0
    from cohort_base c
    where not exists (
        select 1 from {{ config.name }}_target_stream t
        where c.join_key = t.join_key and t.ts > c.ts
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts <= dateadd('{{ config.window.split()[1] }}', {{ config.window.split()[0] }}, c.ts)
        {% endif %}
    )
    
    {% elif config.join_type == 'last_after' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_to_target,
        1 as target_occurred
    from cohort_base c
    inner join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id, t2.ts, t2.feature_json, t2.activity_occurrence,
            row_number() over (partition by c2.activity_id order by t2.ts desc) as rn
        from cohort_base c2
        inner join {{ config.name }}_target_stream t2 
            on c2.join_key = t2.join_key and t2.ts > c2.ts
            {% if config.get('window') and config.window != 'unlimited' %}
            and t2.ts <= dateadd('{{ config.window.split()[1] }}', {{ config.window.split()[0] }}, c2.ts)
            {% endif %}
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, null, 0
    from cohort_base c
    where not exists (
        select 1 from {{ config.name }}_target_stream t
        where c.join_key = t.join_key and t.ts > c.ts
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts <= dateadd('{{ config.window.split()[1] }}', {{ config.window.split()[0] }}, c.ts)
        {% endif %}
    )
    
    {% elif config.join_type == 'aggregate_after' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_base c
    left join {{ config.name }}_target_stream t on c.join_key = t.join_key and t.ts > c.ts
        {% if config.get('window') and config.window != 'unlimited' %}
        and t.ts <= dateadd('{{ config.window.split()[1] }}', {{ config.window.split()[0] }}, c.ts)
        {% endif %}
    group by c.activity_id, c.join_key
    
    {% elif config.join_type == 'first_in_between' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_since_cohort_start,
        datediff('day', t.ts, c.next_cohort_ts) as days_to_cohort_end,
        1 as target_occurred
    from cohort_with_next c
    inner join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id, t2.ts, t2.feature_json, t2.activity_occurrence,
            row_number() over (partition by c2.activity_id order by t2.ts asc) as rn
        from cohort_with_next c2
        inner join {{ config.name }}_target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts > c2.ts
            and (c2.next_cohort_ts is null or t2.ts < c2.next_cohort_ts)
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, null, null, 0
    from cohort_with_next c
    where not exists (
        select 1 from {{ config.name }}_target_stream t
        where c.join_key = t.join_key and t.ts > c.ts
        and (c.next_cohort_ts is null or t.ts < c.next_cohort_ts)
    )
    
    {% elif config.join_type == 'last_in_between' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_since_cohort_start,
        datediff('day', t.ts, c.next_cohort_ts) as days_to_cohort_end,
        1 as target_occurred
    from cohort_with_next c
    inner join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id, t2.ts, t2.feature_json, t2.activity_occurrence,
            row_number() over (partition by c2.activity_id order by t2.ts desc) as rn
        from cohort_with_next c2
        inner join {{ config.name }}_target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts > c2.ts
            and (c2.next_cohort_ts is null or t2.ts < c2.next_cohort_ts)
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    union all
    select 
        c.activity_id, c.join_key, null, null, null, null, null, null, 0
    from cohort_with_next c
    where not exists (
        select 1 from {{ config.name }}_target_stream t
        where c.join_key = t.join_key and t.ts > c.ts
        and (c.next_cohort_ts is null or t.ts < c.next_cohort_ts)
    )
    
    {% elif config.join_type == 'aggregate_in_between' %}
    select 
        c.activity_id as cohort_activity_id,
        c.join_key,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_with_next c
    left join {{ config.name }}_target_stream t on c.join_key = t.join_key
        and t.ts > c.ts
        and (c.next_cohort_ts is null or t.ts < c.next_cohort_ts)
    group by c.activity_id, c.join_key
    {% endif %}
){% if not loop.last %},{% endif %}
{% endfor %}

-- Final assembly
select 
    c.activity_id, c.customer, c.anonymous_customer_id, c.activity, c.ts,
    c.revenue_impact, c.link, c.feature_json, c.activity_occurrence, c.activity_repeated_at
    {% for config in join_configs %}
    {% if config.join_type in ['first_ever', 'last_ever', 'first_before', 'last_before', 'first_after', 'last_after', 'first_in_between', 'last_in_between'] %}
    , {{ config.name }}.target_activity_id as {{ config.name }}_activity_id
    , {{ config.name }}.target_timestamp as {{ config.name }}_timestamp
    , {{ config.name }}.target_features as {{ config.name }}_features
    , {{ config.name }}.target_occurrence as {{ config.name }}_occurrence
    , {{ config.name }}.target_occurred as {{ config.name }}_occurred
    {% if config.join_type in ['first_before', 'last_before'] %}
    , {{ config.name }}.days_since_target as days_since_{{ config.name }}
    {% elif config.join_type in ['first_after', 'last_after'] %}
    , {{ config.name }}.days_to_target as days_to_{{ config.name }}
    {% elif config.join_type.endswith('_in_between') %}
    , {{ config.name }}.days_since_cohort_start as {{ config.name }}_days_since_start
    , {{ config.name }}.days_to_cohort_end as {{ config.name }}_days_to_end
    {% endif %}
    {% elif config.join_type.startswith('aggregate') %}
    , {{ config.name }}.target_count as {{ config.name }}_count
    , {{ config.name }}.first_target_timestamp as {{ config.name }}_first_timestamp
    , {{ config.name }}.last_target_timestamp as {{ config.name }}_last_timestamp
    , {{ config.name }}.total_target_revenue as {{ config.name }}_total_revenue
    , {{ config.name }}.target_occurred as {{ config.name }}_occurred
    , {{ config.name }}.max_target_occurrence as {{ config.name }}_max_occurrence
    {% endif %}
    {% endfor %}
from cohort_base c
{% for config in join_configs %}
left join {{ config.name }}_join {{ config.name }} on c.activity_id = {{ config.name }}.cohort_activity_id
{% endfor %}

{% endmacro %}