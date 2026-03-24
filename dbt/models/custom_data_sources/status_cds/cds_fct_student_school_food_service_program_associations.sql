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
-- Student food services program service model
food_services_prog as (
  select stu_food_ser.k_student, stu_food_ser.ed_org_id, 
         stu_food_ser.k_program, stu_food_ser.k_student_program
  from {{ ref('stg_ef3__student_school_food_service_program_association') }} stu_food_ser
),
-- Student food services program association errors will be here if added in future
unioned_errors as (
    -- Create grain at student level for all stacked_program_services
    select food_services_prog.k_student, food_services_prog.ed_org_id,
           food_services_prog.k_program, food_services_prog.k_student_program, max(student_errors.tdoe_severity_code) as tdoe_severity_code
    from food_services_prog
    left outer join program_errors on program_errors.k_program = food_services_prog.k_program
    left outer join student_errors on student_errors.k_student = food_services_prog.k_student 
    group by food_services_prog.k_student, food_services_prog.ed_org_id,
           food_services_prog.k_program, food_services_prog.k_student_program
    
    union all
    -- Create grain at program level for all stacked_program_services
    select food_services_prog.k_student, food_services_prog.ed_org_id,
           food_services_prog.k_program, food_services_prog.k_student_program, max(program_errors.tdoe_severity_code) as tdoe_severity_code
    from food_services_prog
    left outer join program_errors on program_errors.k_program = food_services_prog.k_program
    group by food_services_prog.k_student, food_services_prog.ed_org_id, food_services_prog.k_program, food_services_prog.k_student_program

    -- program assocaition errors at correct grain will be here if added in future    
)
select k_student, ed_org_id, k_program, k_student_program, 
       max(tdoe_severity_code) as tdoe_severity_code,
       {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from unioned_errors
group by k_student, ed_org_id, k_program, k_student_program
