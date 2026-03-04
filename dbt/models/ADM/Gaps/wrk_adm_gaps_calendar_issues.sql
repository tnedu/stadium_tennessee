{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with bad_cals as (
    select school_year, k_school, k_school_calendar, 
        count(*) as reason_count,
        concat_ws('\n', collect_list(concat('\t', error))) as errors
    from {{ ref('calendars') }} x
    where tdoe_severity = 'critical'
    group by school_year, k_school, k_school_calendar
),
bell_schedule_periods_dates as (
    select bs.k_bell_schedule, bs.k_school, bs.school_year, bs.tenant_code, bs.api_year,
        bs.bell_schedule_name, bs.school_id, bsd.calendar_date
    from {{ ref('stg_ef3__bell_schedules') }} bs
    join {{ ref('stg_ef3__bell_schedules__dates') }} bsd
        on bsd.tenant_code = bs.tenant_code
        and bsd.k_school = bs.k_school
        and bsd.k_bell_schedule = bs.k_bell_schedule
),
school_days as (
    select cal_dates.tenant_code, cal_dates.school_year, cal.k_school, cal_dates.school_id, cal_dates.calendar_date
    from {{ ref('stg_ef3__calendar_dates') }} cal_dates
    join {{ ref('stg_ef3__calendars') }} cal
        on cal.k_school_calendar = cal_dates.k_school_calendar
    join {{ ref('stg_ef3__calendar_dates__calendar_events') }} cal_events
        on cal_events.api_year = cal_dates.api_year
        and cal_events.tenant_code = cal_dates.tenant_code
        and cal_events.k_calendar_date = cal_dates.k_calendar_date
    where cal_events.calendar_event like 'ID%'
),
school_days_with_no_bsdates as (
    select sd.*
    from school_days sd
    where not exists (
            select 1
            from bell_schedule_periods_dates bsd
            where sd.school_year = bsd.school_year
                and sd.school_id = bsd.school_id
                and sd.tenant_code = bsd.tenant_code
                and sd.calendar_date = bsd.calendar_date
        )
)
select school_year, null as k_student, k_school, null as is_primary_school,
    'calendar' as reason_type,
    reason_count,
    concat('School has the following Calendar errors:\n', errors) as possible_reason
from bad_cals
union all
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'calendar' as reason_type,
    reason_count,
    concat('Student tied to calendar with the following errors:\n', x.errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join bad_cals x
    on x.school_year = e.school_year
    and x.k_school = e.k_school
    and x.k_school_calendar = e.k_school_calendar
union
select school_year, null as k_student, k_school, null as is_primary_school,
    'calendar' as reason_type,
    count(*) as reason_count, 
    concat('School ', school_Id, ' has the following School Days which are not found on any Bell Schedule: ',
        array_join(collect_list(calendar_date), ', ')) as possible_reason
from school_days_with_no_bsdates
group by school_year, k_school, school_id