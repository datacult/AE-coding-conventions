with arcee_customers as (
    select zitadel_events.customer
    from {{ ref('stg_zitadel__events') }} as zitadel_events
    where
        zitadel_events.event_type in (
            'user.human.added', 'user.human.selfregistered'
        )
        and zitadel_events.email like '%arcee%'
),

ordered_pvs as (
    select
        session_id,
        distinct_id,
        ts,
        url,
        path,
        referrer,
        search_engine,
        device,
        properties,
        'started_session' as activity,
        row_number() over (
            partition by session_id
            order by ts asc
        ) as rn
    from {{ ref('stg_posthog__events') }}
    where
        event = '$pageview'
        and activity_id <> '0196ae40-f119-7a12-8c0f-743557a569eb'
        and not regexp_like(
            host, '.*(arcee.*dev|dev.*arcee|localhost|127\.0\.0\.1).*', 'i'
        )
        --filters out hosts with localhost, dev-arcee
        and distinct_id not in (
            select arcee_customers.customer
            from arcee_customers
        )
),

final as (

    select
        ordered_pvs.session_id as activity_id,
        ordered_pvs.activity,
        ordered_pvs.distinct_id as anonymous_customer_id,
        ordered_pvs.path as feature_path,
        ordered_pvs.referrer as feature_referrer,
        ordered_pvs.search_engine as feature_search_engine,
        ordered_pvs.device as feature_device,
        null as revenue_impact,
        ordered_pvs.url as link,

        -- Extract UTMs once
        dateadd(millisecond, -1, ordered_pvs.ts) as ts,
        case
            when len(ordered_pvs.distinct_id) < 20 then ordered_pvs.distinct_id
        end as customer,
        regexp_substr(ordered_pvs.url, 'utm_source=([^&]+)', 1, 1, 'e', 1)
            as feature_utm_source,
        regexp_substr(ordered_pvs.url, 'utm_medium=([^&]+)', 1, 1, 'e', 1)
            as feature_utm_medium,
        regexp_substr(ordered_pvs.url, 'utm_campaign=([^&]+)', 1, 1, 'e', 1)
            as feature_utm_campaign,

        -- Clean channel logic
        regexp_substr(ordered_pvs.url, 'utm_content=([^&]+)', 1, 1, 'e', 1)
            as feature_utm_content,

        regexp_substr(ordered_pvs.url, 'utm_term=([^&]+)', 1, 1, 'e', 1)
            as feature_utm_term,

        case
            when
                lower(feature_utm_source) like '%adwo%'
                and (
                    lower(feature_utm_medium) in ('cpc', 'ppc')
                    or feature_utm_medium is null
                )
                or (
                    lower(feature_utm_source) = 'ppc'
                    and (
                        lower(feature_utm_medium) like '%reddit%'
                        or lower(feature_utm_medium) like '%search%'
                    )
                )
                then 'Paid Search'

            when
                lower(feature_utm_medium) like '%paid%'
                and lower(feature_utm_source) in (
                    'reddit', 'linkedin', 'twitter', 'youtube', 'x'
                )
                then 'Paid Social'

            when lower(feature_utm_medium) in ('social', 'zalo', 'weconnect')
                then 'Organic Social'

            when lower(feature_utm_medium) = 'newsletter'
                then 'Paid Media - Newsletter'

            when
                lower(feature_utm_medium) like '%email%'
                or lower(feature_utm_medium) like '%newsletter%'
                then 'Email Marketing'

            when lower(feature_utm_medium) in ('blog', 'partner content')
                then 'Partner Marketing'

            when lower(feature_utm_medium) = 'influencer'
                then 'Influencer Marketing'

            when
                lower(feature_utm_medium) in (
                    'referral', 'iframely', 'getro.com', 'soverin.ai'
                )
                or lower(feature_utm_source) in (
                    'appengine.ai', 'levels.fyi', 'aiagentsdirectory.com'
                )
                then 'Referral'

            --when (lower(feature_utm_medium) = 'website' 
            when (
                lower(feature_utm_medium) = 'website'
                or regexp_like(
                    lower(feature_utm_source),
                    '.*(demo|invent|video|built|event|website).*'
                )
                -- Some of the organic channels are internal traffic
                or feature_utm_medium is null
            )
                then 'Organic'

        end as feature_channel,
        case
            when
                lower(feature_utm_medium) in (
                    'cpc',
                    'ppc',
                    'paidsocial',
                    'paid',
                    'newsletter',
                    'influencer',
                    'partner content',
                    'blog'
                )
                or lower(feature_utm_medium) like '%reddit%'
                or (
                    lower(feature_utm_source) like '%adwo%'
                    and feature_utm_medium is null
                )
                or lower(feature_utm_source) in ('ppc')
                then 'Paid'
            else 'Organic'
        end as feature_traffic_type

    from ordered_pvs
    where ordered_pvs.rn = 1
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
        'utm_source', feature_utm_source,
        'utm_medium', feature_utm_medium,
        'utm_campaign', feature_utm_campaign,
        'utm_content', feature_utm_content,
        'utm_term', feature_utm_term,
        'channel', feature_channel,
        'traffic_type', feature_traffic_type,
        'path', feature_path,
        'referrer', feature_referrer,
        'search_engine', feature_search_engine,
        'device', feature_device
    ) as feature_json
from final