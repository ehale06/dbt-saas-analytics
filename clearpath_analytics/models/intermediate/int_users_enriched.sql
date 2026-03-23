with users as (
    select * from {{ ref('stg_users') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

joined as (
    select
        u.user_id,
        u.account_id,
        u.email,
        u.role,
        u.created_date,
        u.last_login_date,
        u.days_since_signup_to_last_login,
        a.account_name,
        a.plan,
        a.plan_rank,
        a.mrr,
        a.industry,
        a.account_status
    from users u
    left join accounts a
        on u.account_id = a.account_id
)

select * from joined