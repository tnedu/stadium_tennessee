{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3002 %}

/* Students with below LEP Codes must have Date Entered US populated. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    where k_lea is not null
        and lep_code in ('L','W','1','2','3','4','F','N')
        and exists (
            select 1
            from brule
            where cast(seoa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
valid_enrollents_minus_zeroday_early_grads as (
    select *
    from {{ ref('valid_enrollments') }}
    where is_zeroday_early_graduate = 0
),
errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        brule.tdoe_error_code as error_code,
        concat('ELB Student ', 
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'with LEP codes [L, W, 1, 2, 3, 4, F, N] require Date Entered US on District level Student/EdOrg Associations.') as error
    from stg_student_edorgs se
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on se.k_student = s.k_student
    join brule
        on se.school_year between brule.error_school_year_start and brule.error_school_year_end
    where s.date_entered_us is null
        /* We only want this rule to fire if there exists an enrollment that is non-zero-day early grad. */
        and exists (
            select 1
            from valid_enrollents_minus_zeroday_early_grads x
            where se.k_student = x.k_student
                and se.k_lea = x.k_lea
        )
)
select errors.*,
    {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
    brule.tdoe_severity
from errors errors
join brule
    on errors.school_year between brule.error_school_year_start and brule.error_school_year_end