{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3704 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
speds as (
    select c.*,
        brule.tdoe_error_code as potential_tdoe_error_code,
        brule.tdoe_severity as potential_tdoe_severity
    from {{ ref('stg_ef3__student_special_education_program_associations') }} c
    join brule brule
        on cast(c.api_year as int) between brule.error_school_year_start and brule.error_school_year_end
),
speds_at_districts_wo_enrollments as (
    select *
    from speds speds
    where not exists (
        select 1
        from {{ ref('valid_enrollments') }} enrollments
        where enrollments.k_student = speds.k_student
            and enrollments.k_lea = speds.k_lea
            and enrollments.school_year = speds.school_year
    )
),
errors as (
    select distinct bad.k_student_program, bad.k_program, bad.k_student, bad.k_lea, bad.k_school, bad.school_year, 
        bad.program_enroll_begin_date,
        bad.ed_org_id, stu.student_unique_id, stu.state_student_id as legacy_state_student_id,
        bad.potential_tdoe_error_code as error_code,
        concat('Student ', stu.student_unique_id, ' (', stu.state_student_id, ') has SPED Associations at District ', bad.ed_org_id,
            ' but with no valid Enrollments at this District.') as error,
        {{ severity_to_severity_code_case_clause('bad.potential_tdoe_severity') }},
        bad.potential_tdoe_severity as tdoe_severity
    from speds_at_districts_wo_enrollments bad
    join {{ ref('stg_ef3__students') }} stu
        on stu.k_student = bad.k_student
)
select * from errors