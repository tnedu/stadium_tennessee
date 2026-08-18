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
/* Assign report periods for all calendar dates. also if rep_period > 9 then 9. */
cal_dates_with_report_periods as (
    select  
        k_calendar_date, 
        k_school_calendar, 
        calendar_date, 
        is_school_day,
        case
            when report_period <= 9 then report_period
            else 9
        end as report_period
    from (
        select k_calendar_date, k_school_calendar, calendar_date, is_school_day,
                ceiling(row_number() over (
                    partition by k_school_calendar, is_school_day
                    order by calendar_date) / 20) as report_period
        from formatted
    )x
),
/* Idenitify the report_period_begin_date and report_period_end_date for report_periods. 
    As Lead takes next row, only rep_periods are collapsed to one row.*/
cal_report_periods as (
    select k_school_calendar, 
            report_period,
            report_period_begin_date,
            date_sub(lead(report_period_begin_date) over (
                partition by k_school_calendar
                order by report_period), 1) as report_period_end_date
    from (
        select distinct
            k_school_calendar, 
            report_period,
            min(calendar_date) over (
                partition by k_school_calendar, report_period) as report_period_begin_date
        from cal_dates_with_report_periods
    ) x
)
select 
    formatted.k_school_calendar,
    formatted.k_calendar_date,
    rp.report_period,
    row_number() over (
        partition by formatted.k_school_calendar, cal_rp.report_period
        order by formatted.calendar_date) as day_of_report_period,
    CASE formatted.is_school_day WHEN true THEN
        row_number() over (
            partition by formatted.k_school_calendar, cal_rp.report_period, formatted.is_school_day
            order by formatted.calendar_date) 
    ELSE NULL END as school_day_of_report_period,
    rp.report_period_begin_date,
    coalesce(rp.report_period_end_date, 
        max(formatted.calendar_date) over (
        partition by formatted.k_school_calendar, cal_rp.report_period)) as report_period_end_date,
    CASE formatted.is_school_day WHEN true THEN
        count(*) over (
            partition by formatted.k_school_calendar, cal_rp.report_period, formatted.is_school_day
            rows between unbounded preceding and unbounded following) 
    ELSE NULL END as school_days_in_report_period,
    count(*) over (
            partition by formatted.k_school_calendar, cal_rp.report_period
            rows between unbounded preceding and unbounded following) as days_in_the_report_period
    from formatted
    join cal_dates_with_report_periods cal_rp
        on formatted.k_school_calendar = cal_rp.k_school_calendar
        and formatted.k_calendar_date = cal_rp.k_calendar_date
        and formatted.calendar_date = cal_rp.calendar_date
        and formatted.is_school_day = cal_rp.is_school_day
    join cal_report_periods rp
        on cal_rp.k_school_calendar = rp.k_school_calendar
        and cal_rp.report_period = rp.report_period