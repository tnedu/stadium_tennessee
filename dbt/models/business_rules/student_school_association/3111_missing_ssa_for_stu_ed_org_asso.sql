{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3111 %}

/* brule as per standard */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_edorgs as (
    select 
        seoa.*,
        brule.tdoe_error_code,
        brule.tdoe_severity
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    join brule
        on cast(seoa.school_year as int)
           between brule.error_school_year_start and brule.error_school_year_end
    where seoa.k_lea is not null
),
/* All Student Enrollment records. */
stg_student_school_associations as (
    select *,
            {{ get_district_from_school_id('ssa.school_id') }}  as ed_org_id
    from {{ ref('stg_ef3__student_school_associations') }} ssa
)
/* Student/EdOrg Associations that does not have corresponding Student Enrollments. */
select se.k_student, 
        null as k_school,
        null as k_school_calendar,
        se.ed_org_id,
        se.student_unique_id,
        se.school_year, 
        null as entry_date,
        null as entry_grade_level,
        null as calendar_code,
        s.state_student_id as legacy_state_student_id,
        se.tdoe_error_code as error_code,
        concat('Student ', 
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'require Student School Association on District ', se.ed_org_id,
            ' level Student/EdOrg Associations.') as error,
        {{severity_to_severity_code_case_clause('se.tdoe_severity') }},
        se.tdoe_severity
    from stg_student_edorgs se
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on se.k_student = s.k_student
    where not exists (
            select 1 
            from stg_student_school_associations ssa
            where se.k_student = ssa.k_student
            and cast(se.school_year as int)  = cast(ssa.school_year as int) 
            and se.ed_org_id = ssa.ed_org_id
        )