{{
  config(
    materialized = "table",
    schema = "build"
  )
}}
select k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year, entry_date, exit_withdraw_date,
    report_period, report_period_begin_date, report_period_end_date, count(*) as scheduled_school_days, sum(class_duration) as sum_class_duration
from {{ ref('bld_ef3__student_section_days') }}
group by k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year, entry_date, exit_withdraw_date,
    report_period, report_period_begin_date, report_period_end_date
