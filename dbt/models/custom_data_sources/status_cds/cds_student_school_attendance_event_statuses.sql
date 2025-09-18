{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select distinct saes.k_student, saes.k_school, saes.k_session, cd.k_calendar_date, saes.tdoe_severity_code, saes.tdoe_severity
    from {{ ref('cds_student_attendance_event_statuses') }} saes
    join {{ ref('stg_ef3__student_school_associations') }} ssa
        on ssa.k_student = saes.k_student
        and ssa.k_school = saes.k_school
    join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = ssa.k_school_calendar
        and cd.calendar_date = saes.attendance_event_date
)
select k_student, k_school, k_session, k_calendar_date,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_school, k_session, k_calendar_date