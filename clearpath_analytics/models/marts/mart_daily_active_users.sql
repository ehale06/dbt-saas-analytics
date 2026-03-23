with events as (
    select * from {{ ref('int_events_enriched') }}
),

daily_active_users as (
    select
        event_date,
        plan,
        industry,
        count(distinct user_id) as dau,
        count(distinct account_id) as active_accounts,
        count(*) as total_events,
        count(case when event_category = 'core_action' then 1 end) as core_actions,
        count(case when event_category = 'reporting' then 1 end) as reporting_actions,
        count(case when event_category = 'collaboration' then 1 end) as collaboration_actions
    from events
    group by 1, 2, 3
)

select * from daily_active_users