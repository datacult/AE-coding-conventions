{% macro temporal_join(
    cohort_activity, 
    target_activity, 
    join_type, 
    window='all',
    cohort_filter='all',
    between_start_activity=none,
    between_end_activity=none
) %}

{%- set valid_join_types = [
    'first_ever', 'last_ever', 'aggregate_all_ever',
    'first_before', 'last_before', 'aggregate_before', 
    'first_after', 'last_after', 'aggregate_after',
    'first_in_between', 'last_in_between', 'aggregate_in_between'
] -%}

{%- if join_type not in valid_join_types -%}
    {{ exceptions.raise_compiler_error("Invalid join_type: " ~ join_type ~ ". Must be one of: " ~ valid_join_types | join(', ')) }}
{%- endif -%}

{%- if join_type.endswith('_in_between') and (between_start_activity is none or between_end_activity is none) -%}
    {{ exceptions.raise_compiler_error("between_start_activity and between_end_activity must be specified for 'in_between' join types") }}
{%- endif -%}

with cohort_stream_raw as (
    select 
        *,
        coalesce(customer, anonymous_customer_id) as join_key
    from {{ ref('person_stream_' ~ cohort_activity) }}
),

cohort_stream as (
    select * from cohort_stream_raw
    {% if cohort_filter == 'first' %}
    where activity_occurrence = 1
    {% elif cohort_filter == 'last' %}
    where activity_repeated_at is null
    {% elif cohort_filter == 'nth' %}
    where activity_occurrence = {{ cohort_occurrence_number | default(1) }}
    {% endif %}
),

target_stream as (
    select 
        *,
        coalesce(customer, anonymous_customer_id) as join_key  
    from {{ ref('person_stream_' ~ target_activity) }}
),

{% if join_type.endswith('_in_between') %}
-- For in_between joins, we need to determine the boundaries for each cohort activity
cohort_with_next as (
    select 
        *,
        lead(ts) over (partition by join_key order by ts) as next_cohort_ts
    from cohort_stream
),
{% endif %}

joined_data as (
    {% if join_type == 'first_ever' %}
    -- FIRST EVER: Get the very first occurrence of target activity for each customer, regardless of timing
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_stream c
    left join (
        select 
            join_key,
            activity_id,
            ts,
            feature_json,
            activity_occurrence,
            row_number() over (partition by join_key order by ts) as rn
        from target_stream
    ) t on c.join_key = t.join_key and t.rn = 1
    
    {% elif join_type == 'last_ever' %}
    -- LAST EVER: Get the very last occurrence of target activity for each customer, regardless of timing
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_stream c
    left join (
        select 
            join_key,
            activity_id,
            ts,
            feature_json,
            activity_occurrence,
            row_number() over (partition by join_key order by ts desc) as rn
        from target_stream
    ) t on c.join_key = t.join_key and t.rn = 1
    
    {% elif join_type == 'aggregate_all_ever' %}
    -- AGGREGATE ALL EVER: Sum/count all target activities for each customer, regardless of timing
    select 
        c.*,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_stream c
    left join target_stream t on c.join_key = t.join_key
    group by 
        c.activity_id, c.customer, c.anonymous_customer_id, c.activity, c.ts,
        c.revenue_impact, c.link, c.feature_json, c.activity_occurrence, 
        c.activity_repeated_at, c.join_key
    
    {% elif join_type == 'first_before' %}
    -- FIRST BEFORE: Get the FIRST target activity that happened BEFORE each cohort activity
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', t.ts, c.ts) as days_since_target,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_stream c
    left join (
        select 
            join_key,
            activity_id,
            ts,
            feature_json,
            activity_occurrence,
            row_number() over (partition by join_key order by ts) as rn
        from target_stream
    ) t on c.join_key = t.join_key 
        and t.ts < c.ts  -- Target must be BEFORE cohort
        and t.rn = 1     -- FIRST occurrence overall
        {% if window != 'all' %}
        and t.ts >= dateadd('{{ window.split()[1] }}', -{{ window.split()[0] }}, c.ts)
        {% endif %}
    
    {% elif join_type == 'last_before' %}
    -- LAST BEFORE: Get the LAST target activity that happened BEFORE each cohort activity
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', t.ts, c.ts) as days_since_target,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_stream c
    left join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id,
            t2.ts,
            t2.feature_json,
            t2.activity_occurrence,
            row_number() over (
                partition by c2.activity_id 
                order by t2.ts desc  -- LAST = most recent before cohort
            ) as rn
        from cohort_stream c2
        inner join target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts < c2.ts  -- Target must be BEFORE cohort
            {% if window != 'all' %}
            and t2.ts >= dateadd('{{ window.split()[1] }}', -{{ window.split()[0] }}, c2.ts)
            {% endif %}
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    
    {% elif join_type == 'aggregate_before' %}
    -- AGGREGATE BEFORE: Sum/count all target activities that happened BEFORE each cohort activity
    select 
        c.*,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_stream c
    left join target_stream t 
        on c.join_key = t.join_key
        and t.ts < c.ts  -- Target must be BEFORE cohort
        {% if window != 'all' %}
        and t.ts >= dateadd('{{ window.split()[1] }}', -{{ window.split()[0] }}, c.ts)
        {% endif %}
    group by 
        c.activity_id, c.customer, c.anonymous_customer_id, c.activity, c.ts,
        c.revenue_impact, c.link, c.feature_json, c.activity_occurrence, 
        c.activity_repeated_at, c.join_key
    
    {% elif join_type == 'first_after' %}
    -- FIRST AFTER: Get the FIRST target activity that happened AFTER each cohort activity
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_to_target,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_stream c
    left join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id,
            t2.ts,
            t2.feature_json,
            t2.activity_occurrence,
            row_number() over (
                partition by c2.activity_id 
                order by t2.ts asc  -- FIRST = earliest after cohort
            ) as rn
        from cohort_stream c2
        inner join target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts > c2.ts  -- Target must be AFTER cohort
            {% if window != 'all' %}
            and t2.ts <= dateadd('{{ window.split()[1] }}', {{ window.split()[0] }}, c2.ts)
            {% endif %}
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    
    {% elif join_type == 'last_after' %}
    -- LAST AFTER: Get the LAST target activity that happened AFTER each cohort activity
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_to_target,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_stream c
    left join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id,
            t2.ts,
            t2.feature_json,
            t2.activity_occurrence,
            row_number() over (
                partition by c2.activity_id 
                order by t2.ts desc  -- LAST = latest after cohort
            ) as rn
        from cohort_stream c2
        inner join target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts > c2.ts  -- Target must be AFTER cohort
            {% if window != 'all' %}
            and t2.ts <= dateadd('{{ window.split()[1] }}', {{ window.split()[0] }}, c2.ts)
            {% endif %}
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    
    {% elif join_type == 'aggregate_after' %}
    -- AGGREGATE AFTER: Sum/count all target activities that happened AFTER each cohort activity
    select 
        c.*,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_stream c
    left join target_stream t 
        on c.join_key = t.join_key
        and t.ts > c.ts  -- Target must be AFTER cohort
        {% if window != 'all' %}
        and t.ts <= dateadd('{{ window.split()[1] }}', {{ window.split()[0] }}, c.ts)
        {% endif %}
    group by 
        c.activity_id, c.customer, c.anonymous_customer_id, c.activity, c.ts,
        c.revenue_impact, c.link, c.feature_json, c.activity_occurrence, 
        c.activity_repeated_at, c.join_key
    
    {% elif join_type == 'first_in_between' %}
    -- FIRST IN BETWEEN: Get the FIRST target activity between this cohort activity and the NEXT occurrence of the same cohort activity
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_since_cohort_start,
        datediff('day', t.ts, c.next_cohort_ts) as days_to_cohort_end,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_with_next c
    left join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id,
            t2.ts,
            t2.feature_json,
            t2.activity_occurrence,
            row_number() over (
                partition by c2.activity_id 
                order by t2.ts asc  -- FIRST between
            ) as rn
        from cohort_with_next c2
        inner join target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts > c2.ts  -- After this cohort activity
            and (c2.next_cohort_ts is null or t2.ts < c2.next_cohort_ts)  -- Before next cohort activity
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    
    {% elif join_type == 'last_in_between' %}
    -- LAST IN BETWEEN: Get the LAST target activity between this cohort activity and the NEXT occurrence of the same cohort activity
    select 
        c.*,
        t.activity_id as target_activity_id,
        t.ts as target_timestamp,
        t.feature_json as target_features,
        t.activity_occurrence as target_occurrence,
        datediff('day', c.ts, t.ts) as days_since_cohort_start,
        datediff('day', t.ts, c.next_cohort_ts) as days_to_cohort_end,
        case when t.ts is not null then 1 else 0 end as target_occurred
    from cohort_with_next c
    left join (
        select 
            c2.activity_id as cohort_activity_id,
            t2.activity_id,
            t2.ts,
            t2.feature_json,
            t2.activity_occurrence,
            row_number() over (
                partition by c2.activity_id 
                order by t2.ts desc  -- LAST between
            ) as rn
        from cohort_with_next c2
        inner join target_stream t2 
            on c2.join_key = t2.join_key 
            and t2.ts > c2.ts  -- After this cohort activity
            and (c2.next_cohort_ts is null or t2.ts < c2.next_cohort_ts)  -- Before next cohort activity
    ) t on c.activity_id = t.cohort_activity_id and t.rn = 1
    
    {% elif join_type == 'aggregate_in_between' %}
    -- AGGREGATE IN BETWEEN: Sum/count all target activities between this cohort activity and the NEXT occurrence
    select 
        c.*,
        count(t.activity_id) as target_count,
        min(t.ts) as first_target_timestamp,
        max(t.ts) as last_target_timestamp,
        max(t.activity_occurrence) as max_target_occurrence,
        sum(coalesce(t.revenue_impact, 0)) as total_target_revenue,
        case when count(t.activity_id) > 0 then 1 else 0 end as target_occurred
    from cohort_with_next c
    left join target_stream t 
        on c.join_key = t.join_key
        and t.ts > c.ts  -- After this cohort activity
        and (c.next_cohort_ts is null or t.ts < c.next_cohort_ts)  -- Before next cohort activity
    group by 
        c.activity_id, c.customer, c.anonymous_customer_id, c.activity, c.ts,
        c.revenue_impact, c.link, c.feature_json, c.activity_occurrence, 
        c.activity_repeated_at, c.join_key, c.next_cohort_ts
    {% endif %}
)

select * from joined_data

{% endmacro %}