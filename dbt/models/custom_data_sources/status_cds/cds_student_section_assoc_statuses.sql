{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select ssa.k_student, s.k_school, ssa.k_course_section, ssa.tdoe_severity_code, ssa.tdoe_severity
    from {{ ref('student_section_associations') }} ssa
    join {{ ref('stg_ef3__schools') }} s
        on s.school_id = ssa.school_id
)
select k_student, k_school, k_course_section,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_school, k_course_section