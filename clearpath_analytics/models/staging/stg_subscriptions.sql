with source as (
    select * from {{ source('clearpath', 'raw_subscriptions') }}
),

renamed as (
    select
        subscription_id,
        account_id,
        plan,
        mrr,
        started_at::date as started_date,
        ended_at::date as ended_date,
        status,
        case
            when status = 'active' then mrr
            else 0
        end as active_mrr
    from source
)

select * from renamed