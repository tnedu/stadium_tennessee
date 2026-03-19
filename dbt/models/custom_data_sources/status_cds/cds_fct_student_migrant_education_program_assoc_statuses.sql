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
-- Student migrant education program services model
prog_services as (
  select migrant_edu.k_student,  migrant_edu.ed_org_id, 
         migrant_edu.k_program, migrant_edu.k_student_program
  from {{ ref('stg_ef3__student_migrant_education_program_associations') }} migrant_edu
),
-- Student migrant education program association errors will be here if added in future
unioned_errors as (
    -- Create grain at student level for all stacked_program_services
    select student_errors.k_student, prog_services.ed_org_id,
           prog_services.k_program, prog_services.k_student_program, max(student_errors.tdoe_severity_code) as tdoe_severity_code
    from prog_services
    left outer join program_errors on program_errors.k_program = prog_services.k_program
    left outer join student_errors on student_errors.k_student = prog_services.k_student 
    group by student_errors.k_student, prog_services.ed_org_id, prog_services.k_program, prog_services.k_student_program
    
    union all
    -- Create grain at program level for all stacked_program_services
    select prog_services.k_student, program_errors.ed_org_id,
           program_errors.k_program, prog_services.k_student_program, max(program_errors.tdoe_severity_code) as tdoe_severity_code
    from prog_services
    left outer join program_errors on program_errors.k_program = prog_services.k_program
    group by prog_services.k_student, program_errors.ed_org_id, program_errors.k_program, prog_services.k_student_program

    -- program assocaition errors at correct grain will be here if added in future    
)
select k_student, ed_org_id, k_program, k_student_program, 
       max(tdoe_severity_code) as tdoe_severity_code,
       {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from unioned_errors
group by k_student, ed_org_id, k_program, k_student_program