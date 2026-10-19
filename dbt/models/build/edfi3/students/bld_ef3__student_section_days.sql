{{
  config(
    materialized = "table",
    schema = "build"
  )
}}
/* One row per enrollment per school day the student has at least one class meeting, with that day's class minutes. Mirrors student_day_sections (distinct section/class period/bell schedule meetings), using stg section associations instead of fct_student_section_association to avoid the data error cycle. */
with enrollments as (
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, cast(ssa.school_id as int) as school_id, ssa.student_unique_id,
        cast(ssa.school_year as int) as school_year, ssa.entry_date, ssa.exit_withdraw_date
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    where exists (
        select 1
        from {{ ref('valid_enrollments') }} ve
        where ve.k_student = ssa.k_student
          and ve.k_school = ssa.k_school
          and ve.k_school_calendar = ssa.k_school_calendar
    )
),
school_days as (
    select enr.k_student, enr.k_school, enr.k_school_calendar, enr.school_id, enr.student_unique_id, enr.school_year, enr.entry_date, enr.exit_withdraw_date,
        rp.calendar_date, rp.report_period, rp.report_period_begin_date, rp.report_period_end_date
    from enrollments enr
    join {{ ref('bld_ef3__calendar_report_periods') }} rp
        on rp.k_school_calendar = enr.k_school_calendar
       and rp.school_day_of_report_period is not null
       and rp.calendar_date >= enr.entry_date
       and (enr.exit_withdraw_date is null or rp.calendar_date < enr.exit_withdraw_date)
),
student_classes as (
    select sd.k_student, sd.k_school, sd.k_school_calendar, sd.school_id, sd.student_unique_id, sd.school_year, sd.entry_date, sd.exit_withdraw_date,
        sd.calendar_date, sd.report_period, sd.report_period_begin_date, sd.report_period_end_date,
        si.course_code, ssea.k_course_section, ssea.begin_date as ssea_begin_date, si.class_period_name, si.bell_schedule_name, si.start_time, si.end_time, coalesce(si.period_duration, 0) as period_duration
    from school_days sd
    join {{ ref('stg_ef3__student_section_associations') }} ssea
        on ssea.k_student = sd.k_student
       and cast(ssea.school_year as int) = sd.school_year
       and cast(ssea.school_id as int) = sd.school_id
       and sd.calendar_date >= ssea.begin_date
       and (ssea.end_date is null or sd.calendar_date <= ssea.end_date)
    join {{ ref('course_section_days') }} si
        on si.k_course_section = ssea.k_course_section
       and si.k_school = sd.k_school
       and cast(si.school_year as int) = sd.school_year
       and si.calendar_date = sd.calendar_date
    group by sd.k_student, sd.k_school, sd.k_school_calendar, sd.school_id, sd.student_unique_id, sd.school_year, sd.entry_date, sd.exit_withdraw_date,
        sd.calendar_date, sd.report_period, sd.report_period_begin_date, sd.report_period_end_date,
        si.course_code, ssea.k_course_section, ssea.begin_date, si.class_period_name, si.bell_schedule_name, si.start_time, si.end_time, coalesce(si.period_duration, 0)
)
select k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year, entry_date, exit_withdraw_date,
    calendar_date, report_period, report_period_begin_date, report_period_end_date, sum(period_duration) as class_duration
from student_classes
group by k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year, entry_date, exit_withdraw_date,
    calendar_date, report_period, report_period_begin_date, report_period_end_date
