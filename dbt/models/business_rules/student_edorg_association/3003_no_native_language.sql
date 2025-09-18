{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3003 %}

/* Students are required to have Native Language. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    where k_lea is not null
        {{ school_year_exists(error_code, 'seoa') }}
),
errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        {{ error_code }} as error_code,
        concat('Native Language for Student ', 
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'is required on District level Student/EdOrg Associations.') as error
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
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}