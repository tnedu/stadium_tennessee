{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}
{% set error_code = 3009 %}

/* Funding ineligible Student Characteristics must have a Begin Date. No enrollment validation, same as 3006/3007. */
with brule as (
    select tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
stg_student_edorgs as (
    select seoa.k_student, seoa.k_lea, seoa.k_school, seoa.school_year, seoa.ed_org_id, seoa.student_unique_id,
        brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    join brule
        on cast(seoa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where seoa.k_lea is not null
),
funding_ineligible_characteristics as (
    /* Which characteristics are funding ineligible, by school year, is controlled by the seed student_characteristics_attributes.is_funding_ineligible. */
    select student_characteristic,
        cast(stuchar_school_year_start as int) as stuchar_school_year_start,
        cast(ifnull(stuchar_school_year_end, 9999) as int) as stuchar_school_year_end
    from {{ ref('student_characteristics_attributes') }}
    where is_funding_ineligible = 'Y'
),
errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        se.tdoe_error_code as error_code,
        concat('Student ', se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'is missing a required begin date for the ', sc.student_characteristic, ' ',
            'student characteristic.') as error,
        {{ severity_to_severity_code_case_clause('se.tdoe_severity') }},
        se.tdoe_severity
    from stg_student_edorgs se
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on se.k_student = s.k_student
    join {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
        on sc.k_lea = se.k_lea
        and sc.k_student = se.k_student
    join funding_ineligible_characteristics fic
        on fic.student_characteristic = sc.student_characteristic
        and cast(se.school_year as int) between fic.stuchar_school_year_start and fic.stuchar_school_year_end
    where sc.begin_date is null
)
select *
from errors