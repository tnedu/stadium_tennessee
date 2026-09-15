{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with stg_calendar_date as (
    select * from {{ ref('stg_ef3__calendar_dates') }}
),
stg_calendar_events as (
    select * from {{ ref('stg_ef3__calendar_dates__calendar_events')}}
),
dim_school_calendar as (
    select * from {{ ref('edu_wh', 'dim_school_calendar') }}
),
xwalk_calendar_events as (
    select * from {{ ref('xwalk_calendar_events') }}
),
summarize_calendar_events as (
    select
        stg_calendar_events.k_calendar_date,
        -- if there are multiple events on a day, having at least one
        -- that counts as a school day applies to the whole day
        sum(xwalk_calendar_events.is_school_day::integer) >= {{ var("edu:attendance:num_school_day_calendar_events", 1) }} as is_school_day
    from stg_calendar_events
    join xwalk_calendar_events
        on stg_calendar_events.calendar_event = xwalk_calendar_events.calendar_event_descriptor
    group by 1
),
formatted as (
    select
        stg_calendar_date.k_calendar_date,
        stg_calendar_date.k_school_calendar,
        dim_school_calendar.k_school,
        stg_calendar_date.tenant_code,
        stg_calendar_date.school_year,
        stg_calendar_date.calendar_date,
        summarize_calendar_events.is_school_day
    from stg_calendar_date
    join dim_school_calendar
        on stg_calendar_date.k_school_calendar = dim_school_calendar.k_school_calendar
    join summarize_calendar_events
        on stg_calendar_date.k_calendar_date = summarize_calendar_events.k_calendar_date
),
assign_report_period as (
    select *,
        greatest(
            least(
                ceiling(
                    sum(case when is_school_day then 1 else 0 end) over (
                        partition by k_school_calendar
                        order by calendar_date
                    ) / 20
                )
            , 9)
        , 1) as report_period
    from formatted
),
rp_dates as (
    select k_school_calendar, report_period, report_period_begin_date,
        coalesce(
            cast(dateadd(day, -1,
                    lead(report_period_begin_date) over (partition by k_school_calendar order by report_period)
                ) as date),
            final_report_period_end_date
        ) as report_period_end_date
    from (
        select distinct k_school_calendar, report_period,
            min(calendar_date) over (partition by k_school_calendar, report_period) as report_period_begin_date,
            max(calendar_date) over (partition by k_school_calendar) as final_report_period_end_date
        from assign_report_period
    )
),
final as (
    select 
        arp.k_school_calendar,
        arp.k_calendar_date,
        arp.report_period,
        rp.report_period_begin_date,
        rp.report_period_end_date,
        count(*) over (partition by arp.k_school_calendar, arp.report_period) as days_in_report_period,
        row_number() over (partition by arp.k_school_calendar, arp.report_period order by arp.calendar_date) as day_of_report_period,
        sum(case arp.is_school_day when true then 1 else 0 end) over (partition by arp.k_school_calendar, arp.report_period) as school_days_in_report_period,
        case arp.is_school_day
            when true then 
                sum(case arp.is_school_day when true then 1 else 0 end) over (partition by arp.k_school_calendar, arp.report_period order by arp.calendar_date)
            else null
        end as school_day_of_report_period
    from assign_report_period arp
    join rp_dates rp
        on rp.k_school_calendar = arp.k_school_calendar
        and rp.report_period = arp.report_period
)
select * from final