with source as (

    select * from {{ref('payment_data')}}

),

renamed as (
    Select 
        unique_id as activity_id,
        email as customer, 
        anonymous_customer_id, 
        'purchased_credits' as activity,
        created_at as ts,
        webhook_type as feature_webhook_type,
        payment_status as feature_payment_status,
        total_amount as feature_total_amount,
        email as feature_email

    from source

)

select * from renamed