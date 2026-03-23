with accounts as (
    select * from {{ ref('stg_accounts') }}
),

account_events as (
    select
        account_id,
        count(distinct user_id) as total_users,
        count(*) as total_events,
        count(distinct event_type) as unique_features_used,
        max(event_date) as last_event_date,
        min(event_date) as first_event_date,
        count(case when event_category = 'core_action' then 1 end) as core_actions,
        count(case when event_type = 'invite_member' then 1 end) as members_invited
    from {{ ref('int_events_enriched') }}
    group by 1
),

churn_analysis as (
    select
        a.account_id,
        a.account_name,
        a.plan,
        a.mrr,
        a.industry,
        a.employee_count,
        a.account_status,
        a.created_date,
        a.churned_date,
        datediff('day', a.created_date, coalesce(a.churned_date, current_date)) as days_as_customer,
        e.total_users,
        e.total_events,
        e.unique_features_used,
        e.core_actions,
        e.members_invited,
        e.first_event_date,
        e.last_event_date,
        case
            when a.account_status = 'churned' then 'churned'
            when e.last_event_date < current_date - interval '30 days' then 'at_risk'
            when e.total_events < 10 then 'low_engagement'
            else 'healthy'
        end as health_status
    from accounts a
    left join account_events e
        on a.account_id = e.account_id
)

select * from churn_analysis