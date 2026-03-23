with source as (
    select * from {{ source('clearpath', 'raw_users') }}
),

renamed as (
    select
        user_id,
        account_id,
        email,
        role,
        created_at::date as created_date,
        last_login_at::date as last_login_date,
        datediff('day', created_at, last_login_at) as days_since_signup_to_last_login
    from source
)

select * from renamed