with source as (
    select * from {{ source('clearpath', 'raw_accounts') }}
),

renamed as (
    select
        account_id,
        account_name,
        plan,
        mrr,
        industry,
        employee_count,
        created_at::date as created_date,
        churned_at::date as churned_date,
        case
            when churned_at is not null then 'churned'
            else 'active'
        end as account_status,
        case
            when plan = 'starter' then 1
            when plan = 'growth' then 2
            when plan = 'enterprise' then 3
        end as plan_rank
    from source
)

select * from renamed