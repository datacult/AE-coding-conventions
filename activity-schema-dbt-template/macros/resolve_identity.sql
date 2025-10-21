{% macro resolve_identity() %}
    with base_data as (
        {{ caller() }}
    ),
    
    identity_windows as (
        select * from {{ ref('identity_time_windows') }}
    ),
    
    resolved as (
        select 
            bd.*,
            coalesce(
                bd.customer,  -- Keep original if exists
                iw.customer   -- Apply resolved identity
            ) as resolved_customer,
            
        from base_data bd
        left join identity_windows iw
            on bd.anonymous_customer_id = iw.anonymous_customer_id
            and bd.ts > iw.window_start
            and bd.ts <= iw.window_end
    )
    
    select 
        activity_id,
        anonymous_customer_id,
        resolved_customer as customer,
        activity,
        ts,
        revenue_impact,
        link,
        feature_json,
        row_number() over (partition by  activity, 
            coalesce(resolved_customer, anonymous_customer_id) order by ts asc) as activity_occurrence,
        lead(ts) over ( partition by activity, 
            coalesce(resolved_customer, anonymous_customer_id) order by ts asc) as activity_repeated_at
    from resolved
{% endmacro %}