with source as (

    select * from {{ref('model_usage')}}

),

renamed as (

    select
        uuid as activity_id, 
        email as  customer,
        anonymous_customer_id,
        ts,
        'used_model' as activity,
        case 
            when APIINITIATED then 'API' 
            else 'UI'
        end as feature_source,
        model as feature_model,
        completion_tokens as feature_completion_tokens,
        prompt_token as feature_prompt_tokens

    from source

)

select * from renamed