{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.tdoe_severity_code, ssa.tdoe_severity
    from {{ ref('student_school_associations') }} ssa
    union
    select ssa.k_student, ssa.k_school, y.k_school_calendar, y.tdoe_severity_code, y.tdoe_severity
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join {{ ref('calendars') }} y
        on y.k_school_calendar = ssa.k_school_calendar
)
select k_student, k_school, k_school_calendar,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_school, k_school_calendar