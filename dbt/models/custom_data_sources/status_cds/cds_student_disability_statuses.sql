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
    select k_program, ed_org_id, tdoe_severity_code, tdoe_severity
    from {{ ref('dim_program') }}
),
stu_disabilities as (
    select k_student, ed_org_id, k_program, disability_type
    from {{ ref('bld_ef3__student__disabilities') }}
),
unioned_errors as (
    select stu_disabilities.k_student, stu_disabilities.ed_org_id, stu_disabilities.k_program, max(student_errors.tdoe_severity_code) as tdoe_severity_code
    from stu_disabilities
    left outer join student_errors on stu_disabilities.k_student = student_errors.k_student
    group by stu_disabilities.k_student, stu_disabilities.ed_org_id, stu_disabilities.k_program
    union all
    select stu_disabilities.k_student, program_errors.ed_org_id, program_errors.k_program, max(program_errors.tdoe_severity_code) as tdoe_severity_code
    from stu_disabilities
    left outer join program_errors on stu_disabilities.k_program = program_errors.k_program
    group by stu_disabilities.k_student, program_errors.ed_org_id, program_errors.k_program
)
select k_student, ed_org_id, k_program, max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from unioned_errors
group by k_student, ed_org_id, k_program