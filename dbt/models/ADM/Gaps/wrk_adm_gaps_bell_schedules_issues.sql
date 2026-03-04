{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with q as (
    select sess.school_year, sch.k_school, 
        count(*) as reason_count,
        concat_ws('\n', collect_list(concat('\t', error))) as errors
    from {{ ref('bell_schedules') }} sess
    join {{ ref('stg_ef3__schools') }} sch
        on sch.school_id = sess.school_id
    where tdoe_severity = 'critical'
    group by sess.school_year, sch.k_school
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
    select cal_dates.tenant_code, cal_dates.school_year, cal_dates.school_id, cal_dates.calendar_date
    from {{ ref('stg_ef3__calendar_dates') }} cal_dates
    join {{ ref('stg_ef3__calendar_dates__calendar_events') }} cal_events
        on cal_events.api_year = cal_dates.api_year
        and cal_events.tenant_code = cal_dates.tenant_code
        and cal_events.k_calendar_date = cal_dates.k_calendar_date
    where cal_events.calendar_event like 'ID%'
),
bsdates_missing_school_days as (
    select bsd.school_year, bsd.k_school, bsd.school_id, 
        concat(bsd.bell_schedule_name, ': ', array_join(collect_list(bsd.calendar_date), ', ')) as bad_bs,
        count(*) as date_count
    from bell_schedule_periods_dates bsd
    where not exists (
            select 1
            from school_days sd
            where sd.school_year = bsd.school_year
                and sd.school_id = bsd.school_id
                and sd.tenant_code = bsd.tenant_code
                and sd.calendar_date = bsd.calendar_date
        )
    group by bsd.school_year, bsd.k_school, bsd.school_id, bsd.bell_schedule_name
)
select school_year, null as k_student, k_school, null as is_primary_school,
    'bell schedule' as reason_type,
    reason_count,
    concat('School has the following Bell Schedule errors:\n', errors) as possible_reason
from q
union
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'bell schedule' as reason_type,
    count(*) as reason_count,
    concat('Student is tied to Bell Schedules with the following errors:\n', 
        concat_ws('\n', collect_list(concat('\t', x.error)))) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join {{ ref('stg_ef3__student_section_associations')}} sections
    on sections.school_year = e.school_year
    and sections.k_student = e.k_student
join {{ ref('stg_ef3__sections__class_periods') }} cps
    on cps.k_course_section = sections.k_course_section
join {{ ref('stg_ef3__bell_schedules__class_periods') }} bscps
    on bscps.k_class_period = cps.k_class_period
join {{ ref('class_periods') }} x
    on x.k_class_period = bscps.k_class_period
group by e.school_year, e.k_student, e.k_school, e.is_primary_school
union
select school_year, null as k_student, k_school, null as is_primary_school,
    'bell schedule' as reason_type,
    sum(date_count) as reason_count, 
    concat('School ', school_Id, ' has the following Bell Schedules with Dates that are not School Days:\n',
        concat_ws('\n', collect_list(concat('\t', bad_bs)))) as possible_reason
from bsdates_missing_school_days
group by school_year, k_school, school_id