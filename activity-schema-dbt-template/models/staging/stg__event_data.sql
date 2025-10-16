with source as (

    select * from {{ref('event_data')}}

),

renamed as (

    select
        uuid as activity_id, 
        hash_email as  customer,
        distinct_id as anonymous_customer_id,
        ts,
        case 
            when event = '$pageview' then 'viewed_page'
            when event = '$identify' then 'identify'
        end as activity,
        path as feature_path,
        session_id as feature_session_id

    from source

)

select * from renamed