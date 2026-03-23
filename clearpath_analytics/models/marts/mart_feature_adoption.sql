with events as (
    select * from {{ ref('int_events_enriched') }}
),

feature_usage as (
    select
        account_id,
        account_name,
        plan,
        industry,
        account_status,
        event_type as feature_name,
        event_category as feature_category,
        count(*) as total_uses,
        count(distinct user_id) as unique_users,
        min(event_date) as first_used_date,
        max(event_date) as last_used_date,
        datediff('day', min(event_date), max(event_date)) as days_of_usage_span
    from events
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from feature_usage