{{
  config(
    materialized="table",
    schema="cds"
  )
}}

-- Define all optional program service models here exccept migrant education(cds_exist) and school food service (cds exist)
{% set stage_program_relations = [] %}

--Generic Program Assoc
{% do stage_program_relations.append(ref('stg_ef3__student_program_associations')) %}

-- Special Education
{% if var('src:program:special_ed:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__student_special_education_program_associations')) %}
{% endif %}

-- Language Instruction
{% if var('src:program:language_instruction:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__student_language_instruction_program_associations')) %}
{% endif %}

-- Homeless
{% if var('src:program:homeless:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__student_homeless_program_associations')) %}
{% endif %}

-- Title I Part A
{% if var('src:program:title_i:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__student_title_i_part_a_program_associations')) %}
{% endif %}

-- CTE
{% if var('src:program:cte:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__student_cte_program_associations')) %}
{% endif %}

stacked_program_services as (
    {{ dbt_utils.union_relations(
        relations=stage_program_relations
    ) }}
),

with student_errors as (
    select k_student, tdoe_severity_code, tdoe_severity
    from {{ ref('dim_student') }}
),
program_errors as (
    select k_program, ed_org_id, tdoe_severity_code, tdoe_severity
    from {{ ref('dim_program') }}
),
--homeless_errors at correct grain
homeless_errors as (
    select homeless_assoc.k_student, homeless_assoc.ed_org_id, homeless_assoc.k_program, 
           homeless_assoc.k_student_program, homeless_err.tdoe_severity_code, homeless_err.tdoe_severity
    from {{ ref('stg_ef3__student_homeless_program_associations') }} homeless_assoc
    join {{ ref('student_homeless_program_associations') }} homeless_err
      on homeless_assoc.k_student = homeless_err.k_student
      and homeless_assoc.k_program = homeless_err.k_program
),
program_association_errors as (
  select k_student, ed_org_id, k_program, k_student_program, tdoe_severity_code, tdoe_severity
  from homeless_errors
  union all
  select k_student, ed_org_id, k_program, k_student_program, tdoe_severity_code, tdoe_severity
  from {{ ref('cds_student_migrant_education_program_assoc_statuses') }} migrant_errors
  union all
  select k_student, ed_org_id, k_program, k_student_program, tdoe_severity_code, tdoe_severity
  from {{ ref('cds_student_school_food_service_program_associations') }} food_service_errors
  --other dependent staging tables  will be added here if new business rules and errors are added in future
),
unioned_errors as (
    -- Create grain at student level for all stacked_program_services
    select student_errors.k_student, stacked.ed_org_id,
           stacked.k_program, stacked.k_student_program, max(student_errors.tdoe_severity_code) as tdoe_severity_code
    from stacked_program_services stacked
    left outer join program_errors 
          on program_errors.k_program = stacked.k_program
    left outer join student_errors on student_errors.k_student = stacked.k_student 
    group by student_errors.k_student, stacked.ed_org_id, stacked.k_program, stacked.k_student_program

    union all
    -- Create grain at program level for all stacked_program_services
    select stacked.k_student, program_errors.ed_org_id, program_errors.k_program, 
           stacked.k_student_program, max(program_errors.tdoe_severity_code) as tdoe_severity_code
    from stacked_program_services stacked
    left outer join program_errors 
          on program_errors.k_program = stacked.k_program
    group by stacked.k_student, program_errors.ed_org_id, program_errors.k_program, stacked.k_student_program

    union all
    -- Add program assocaition errors at correct grain
    select k_student, ed_org_id, k_program, k_student_program, max(program_association_errors.tdoe_severity_code) as tdoe_severity_code
    from program_association_errors
    group by k_student, ed_org_id, k_program, k_student_program
)
select k_student, ed_org_id, k_program, k_student_program, max(tdoe_severity_code) as tdoe_severity_code,
       {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from unioned_errors
group by k_student, ed_org_id, k_program, k_student_program