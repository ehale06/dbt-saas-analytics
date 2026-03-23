with events as (
    select * from {{ ref('int_events_enriched') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

monthly_events as (
    select
        event_month,
        account_id,
        account_name,
        plan,
        plan_rank,
        industry,
        account_status,
        count(distinct user_id) as mau,
        count(*) as total_events,
        count(distinct event_type) as unique_features_used,
        count(case when event_category = 'core_action' then 1 end) as core_actions,
        count(case when event_type = 'create_project' then 1 end) as projects_created,
        count(case when event_type = 'complete_task' then 1 end) as tasks_completed,
        count(case when event_type = 'invite_member' then 1 end) as members_invited
    from events
    group by 1, 2, 3, 4, 5, 6, 7
),

joined as (
    select
        m.*,
        a.mrr,
        a.employee_count,
        a.created_date as account_created_date,
        a.churned_date
    from monthly_events m
    left join accounts a
        on m.account_id = a.account_id
)

select * from joined