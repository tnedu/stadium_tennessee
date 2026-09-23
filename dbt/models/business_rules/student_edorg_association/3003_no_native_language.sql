{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3003 %}

/* Students are required to have Native Language. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_edorgs as (
    select seoa.*,
            brule.tdoe_error_code,
            brule.tdoe_severity
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    join brule
        on cast(seoa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where k_lea is not null      
),
valid_enrollments_minus_service_sch as (
    select * from {{ ref('valid_enrollments') }}
        /* we want to ignore service schools for this rule */
        and not exists (
            select 1
            from {{ ref('service_schools') }} ss
            where ssa.k_school = ss.k_school
        )  
),
errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        se.tdoe_error_code as error_code,
        concat('Native Language for Student ', 
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'is required on District level Student/EdOrg Associations.') as error,
        {{ severity_to_severity_code_case_clause('se.tdoe_severity') }},
        se.tdoe_severity
    from stg_student_edorgs se
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on se.k_student = s.k_student
    where 
        not exists (
            select 1
            from {{ ref('stg_ef3__stu_ed_org__languages') }} sl
            where sl.k_lea = se.k_lea
                and sl.k_student = se.k_student
                and sl.language_use in ('Native language', 'Home language', 'Dominant language')
        )
        and exists (
            select 1
            from valid_enrollments_minus_service_sch ve
            where ve.k_student = se.k_student
                and ve.school_year = se.school_year
                and ve.k_lea = se.k_lea
        )
)
select *
from errors