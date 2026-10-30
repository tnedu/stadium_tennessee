{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3010 %}

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
select distinct se.k_student, se.k_lea, cast( null as int ) as k_school,
        cast(se.school_year as int) as school_year, se.ed_org_id, s.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        se.tdoe_error_code as error_code,
        concat('Student ', 
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'associated with District ', se.ed_org_id, ' requires a Student School Association. Or
            remove this Student Association at this District.') as error,
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