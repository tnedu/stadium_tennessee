{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3002 %}

/* Students with below LEP Codes must have Date Entered US populated. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    where k_lea is not null
        and lep_code in ('L','W','1','2','3','4','F','N')
        {{ school_year_exists(error_code, 'seoa') }}
),
errors as (
  select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
      s.state_student_id as legacy_state_student_id,
      {{ error_code }} as error_code,
      concat('ELB Student ', 
          se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
          'with LEP codes [L, W, 1, 2, 3, 4, F, N] require Date Entered US on District level Student/EdOrg Associations.') as error
  from stg_student_edorgs se
  join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
      on se.k_student = s.k_student
  where s.date_entered_us is null
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}