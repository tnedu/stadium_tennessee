{{
  config(
    materialized = "table",
    schema = "build"
  )
}}

with ssas as (
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar,
        cast(ssa.school_id as int) as school_id, ssa.student_unique_id,
        cast(ssa.school_year as int) as school_year, ssa.entry_date,
        ssa.exit_withdraw_date, ssa.studentStandardDays
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    where exists (
        select 1
        from {{ ref('valid_enrollments') }} ve
        where ve.k_student = ssa.k_student
          and ve.k_school = ssa.k_school
          and ve.k_school_calendar = ssa.k_school_calendar
    )
)

select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id,
    ssa.student_unique_id, ssa.school_year, ssa.entry_date, ssa.exit_withdraw_date,
    sd.col.effectiveDate::date as effective_date,
    sd.col.studentStandardDayDuration::int as ssd_duration
from ssas ssa
lateral view explode(ssa.studentStandardDays) sd