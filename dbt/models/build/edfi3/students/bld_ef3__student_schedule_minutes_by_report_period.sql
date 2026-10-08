{{
  config(
    materialized = "table",
    schema = "build"
  )
}}

select ssea.k_student, ssa.k_school, ssa.k_school_calendar, cast(ssea.school_id as int) as school_id,
    ssea.student_unique_id, cast(ssea.school_year as int) as school_year, ssa.entry_date,
    rp.report_period, sum(csd.period_duration) as sum_class_duration
from {{ ref('stg_ef3__student_section_associations') }} ssea
join {{ ref('stg_ef3__student_school_associations') }} ssa
    on ssa.k_student = ssea.k_student
   and ssa.school_year = ssea.school_year
   and ssa.school_id = ssea.school_id
join {{ ref('valid_enrollments') }} ve
    on ve.k_student = ssa.k_student
   and ve.k_school = ssa.k_school
   and ve.k_school_calendar = ssa.k_school_calendar
join {{ ref('course_section_days') }} csd
    on csd.k_course_section = ssea.k_course_section
   and csd.school_year = ssea.school_year
   and csd.calendar_date between ssea.begin_date and ssea.end_date
   and csd.calendar_date >= ssa.entry_date
   and (ssa.exit_withdraw_date is null or csd.calendar_date <= ssa.exit_withdraw_date)
join {{ ref('bld_ef3__calendar_report_periods') }} rp
    on rp.k_school_calendar = ssa.k_school_calendar
   and rp.calendar_date = csd.calendar_date
group by ssea.k_student, ssa.k_school, ssa.k_school_calendar, cast(ssea.school_id as int),
    ssea.student_unique_id, cast(ssea.school_year as int), ssa.entry_date, rp.report_period