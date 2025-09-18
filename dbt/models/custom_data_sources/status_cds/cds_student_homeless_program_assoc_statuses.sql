{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_student, k_program, k_lea, tdoe_severity_code, tdoe_severity
    from {{ ref('student_homeless_program_associations') }}
)
select k_student, k_program, k_lea,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_program, k_lea