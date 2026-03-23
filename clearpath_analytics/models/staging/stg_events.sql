with source as (
    select * from {{ source('clearpath', 'raw_events') }}
),

renamed as (
    select
        event_id,
        user_id,
        account_id,
        event_type,
        occurred_at::date as event_date,
        date_trunc('month', occurred_at)::date as event_month,
        case
            when event_type in ('create_project', 'create_task', 'complete_task', 'invite_member')
                then 'core_action'
            when event_type in ('view_dashboard', 'export_report')
                then 'reporting'
            when event_type in ('comment', 'attach_file')
                then 'collaboration'
            else 'other'
        end as event_category
    from source
)

select * from renamed