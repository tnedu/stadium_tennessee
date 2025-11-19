{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_student, k_school, tdoe_severity_code, tdoe_severity
    from {{ ref('student_school_attendance_events') }}
    where attendance_event_category = 'Student Standard Day'
)
select k_student, k_school,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_school