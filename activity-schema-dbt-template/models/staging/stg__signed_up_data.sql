with source as (

    select * from {{ref('signed_up_data')}}

),

renamed as (

    Select 
        unique_id as activity_id,
        customer, 
        null as anonymous_customer_id, 
        'signed_up' as activity,
        signedup_at as ts,
        hash_email as feature_email
    from source

)

select * from renamed