{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

/* This gets all enrollments. Period. */
with school_day_events as (
    select *
    from {{ ref('xwalk_calendar_events') }}
    where is_school_day = true
)
select ssa.school_year, ssa.k_student, sch.k_lea, ssa.k_school, ssa.school_Id, ssa.k_school_calendar,
    ssa.is_primary_school, ssa.entry_date, ssa.exit_withdraw_date,
    sum(
        case
            when dates.calendar_date is not null then 1
            else 0
        end
    ) as enrolled_days
from {{ ref('stg_ef3__student_school_associations') }} ssa
join {{ ref('stg_ef3__schools') }} sch
    on sch.k_school = ssa.k_school
left outer join {{ ref('stg_ef3__calendar_dates') }} dates
    on dates.school_year = ssa.school_year
    and dates.k_school_calendar = ssa.k_school_calendar
    and dates.calendar_date >= ssa.entry_date
    and (ssa.exit_withdraw_date is null 
        or dates.calendar_date < ssa.exit_withdraw_date)
left outer join {{ ref('stg_ef3__calendar_dates__calendar_events') }} date_events
    on date_events.k_calendar_date = dates.k_calendar_date
    and date_events.k_school_calendar = dates.k_school_calendar
    and date_events.calendar_event in (select calendar_event_descriptor from school_day_events)
group by ssa.k_student, ssa.k_school, sch.k_lea, ssa.school_id, ssa.k_school_calendar,
    ssa.school_year, ssa.is_primary_school, ssa.entry_date, ssa.exit_withdraw_date