{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with student_errors as (
    select k_student, tdoe_severity_code, tdoe_severity
    from {{ ref('dim_student') }}
),
program_errors as (
    select k_program, k_lea, k_school, tdoe_severity_code, tdoe_severity
    from {{ ref('dim_program') }}
),
stu_disabilities as (
    select k_student, k_lea, k_school, k_program, disability_type
    from {{ ref('bld_ef3__student__disabilities') }}
),
unioned_errors as (
    select stu_disabilities.k_student, stu_disabilities.k_lea, stu_disabilities.k_school, stu_disabilities.k_program, max(student_errors.tdoe_severity_code) as tdoe_severity_code
    from stu_disabilities
    left outer join student_errors on stu_disabilities.k_student = student_errors.k_student
    group by stu_disabilities.k_student, stu_disabilities.k_lea, stu_disabilities.k_school, stu_disabilities.k_program
    union all
    select stu_disabilities.k_student, stu_disabilities.k_lea, stu_disabilities.k_school, stu_disabilities.k_program, max(program_errors.tdoe_severity_code) as tdoe_severity_code
    from stu_disabilities
    left outer join program_errors on stu_disabilities.k_program = program_errors.k_program
    group by stu_disabilities.k_student, stu_disabilities.k_lea, stu_disabilities.k_school, stu_disabilities.k_program
)
select k_student, k_lea, k_school, k_program, max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from unioned_errors
group by k_student, k_lea, k_school, k_program