{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_course, k_student_academic_record, tdoe_severity_code, tdoe_severity
    from {{ ref('course_transcripts') }}
)
select k_course, k_student_academic_record,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_course, k_student_academic_record