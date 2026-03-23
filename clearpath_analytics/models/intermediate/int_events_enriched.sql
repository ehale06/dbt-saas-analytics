with events as (
    select * from {{ ref('stg_events') }}
),

users as (
    select * from {{ ref('int_users_enriched') }}
),

joined as (
    select
        e.event_id,
        e.user_id,
        e.account_id,
        e.event_type,
        e.event_category,
        e.event_date,
        e.event_month,
        u.role,
        u.plan,
        u.plan_rank,
        u.industry,
        u.account_status,
        u.account_name
    from events e
    left join users u
        on e.user_id = u.user_id
)

select * from joined