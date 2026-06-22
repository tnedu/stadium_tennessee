{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

/* No show enrollments are simply any non-valid enrollments, I guess. */
select ssa.k_student, ssa.k_school, ssa.k_school_calendar,
    ssa.tenant_code, ssa.api_year, ssa.school_id, ssa.student_unique_id,
    ssa.school_year, ssa.is_primary_school, ssa.entry_date, ssa.exit_withdraw_date,
    ssa.calendar_code, ssa.entry_type, ssa.exit_withdraw_type
from {{ ref('stg_ef3__student_school_associations') }} ssa
where 
    not exists (
        select 1
        from {{ ref('valid_enrollments') }} x
        where x.k_student = ssa.k_student
            and x.k_school = ssa.k_school
            and x.k_school_calendar = ssa.k_school_calendar
            and x.school_year = ssa.school_year
            and x.is_primary_school = ssa.is_primary_school
            and x.entry_date = ssa.entry_date
    )