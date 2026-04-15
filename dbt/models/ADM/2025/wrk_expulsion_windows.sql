{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
Student Expulsions has a start date and then a number of school days. But that's hard to reason out
when joining lots of other tables. So the purpose of this model is to figure out expulsion windows.
That is to say, a student is expelled from Date A to Date B, inclusive. That's much easier to use
when you need to know if they are expelled on any given day (for ADM calculations).
*/

with student_school_disciplines as (
    /* First get the set of disciplines we need to build date ranges for. */
    select distinct k_student, k_school, school_year, tenant_code, discipline_date, discipline_action,
        coalesce(actual_discipline_action_length, discipline_action_length) as discipline_action_length
    from {{ ref('fct_student_discipline_actions') }} 
    where discipline_action in ('E','S')
        and discipline_action_length is not null
        and coalesce(actual_discipline_action_length, discipline_action_length) > 0
),
enrollment_calendars_school_days as (
    /* A discipline record is not associated with an enrollment. There are cases where a student could be expelled 
    but have multiple enrollments on different calendars at the same school. So we need to get the set of CALENDARs
    associated with disciplines along with the school days for each calendar. */
    select distinct fssa.k_school, fssa.k_school_calendar, fssa.school_year, fssa.tenant_code,
        dcd.calendar_date
    from {{ ref('fct_student_school_association') }} fssa
    join {{ ref('dim_calendar_date') }} dcd
        on dcd.k_school_calendar = fssa.k_school_calendar
        and dcd.k_school = fssa.k_school
        and dcd.is_school_day = true
    where exists (
        select 1
        from student_school_disciplines ssd
        where ssd.k_school = fssa.k_school
            and ssd.k_student = fssa.k_student
    )
    order by fssa.k_school, fssa.k_school_calendar, dcd.calendar_date
),
disciplines_calendar_applied as (
    /* We have to apply the disciplines to the CALENDAR so that we can determine the end date for the discipline on that specific calendar. */
    select disc.k_student, disc.k_school, sd.k_school_calendar, disc.school_year, disc.tenant_code, disc.discipline_date, disc.discipline_action,
        disc.discipline_action_length,
        sd.calendar_date,
        row_number() over (
            partition by disc.k_student, disc.k_school, sd.k_school_calendar, disc.school_year, disc.tenant_code, disc.discipline_date, disc.discipline_action
            order by sd.calendar_date) as rn
    from student_school_disciplines disc
    join enrollment_calendars_school_days sd
        on sd.k_school = disc.k_school
        and sd.school_year = disc.school_year
        and sd.calendar_date >= disc.discipline_date
    qualify rn <= discipline_action_length
)
select k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_date, discipline_action, discipline_action_length,
    min(calendar_date) as discipline_date_begin,
    max(calendar_date) as discipline_date_end
from disciplines_calendar_applied
group by k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_date, discipline_action, discipline_action_length

