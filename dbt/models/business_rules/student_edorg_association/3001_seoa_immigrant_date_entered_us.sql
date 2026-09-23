{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3001 %}

/* Students with the IMMIG student_characteristic must have Date Entered US populated. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity,
        rule_model
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
    and rule_model = '{{ this.identifier }}'
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
enrollments_minus_sped_sch_enroll as (
    select ssa.k_student, school.k_lea, school.lea_id, ssa.k_school, ssa.school_id, ssa.k_school_calendar,
        ssa.tenant_code, ssa.api_year,  ssa.student_unique_id,ssa.school_year, ssa.is_primary_school,
        ssa.entry_date, ssa.exit_withdraw_date, ssa.calendar_code
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join {{ ref('stg_ef3__schools') }} school
        on ssa.k_school = school.k_school
    /* we want to ignore service schools for this rule */
    where not exists (
        select 1
        from service_schools ss
        where ssa.k_school = ss.k_school
    ) 
),
errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        se.tdoe_error_code as error_code,
        concat('Immigrant Student ', 
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'requires Date Entered US on District level Student/EdOrg Association.') as error,
        {{ severity_to_severity_code_case_clause('se.tdoe_severity') }},
        se.tdoe_severity
    from stg_student_edorgs se
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on se.k_student = s.k_student
    where se.dateEnteredUS is null
    and exists (
            select 1
            from {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
            where sc.k_lea = se.k_lea
                and sc.k_student = se.k_student
                and sc.student_characteristic = 'IMMIG'
        )
         /* We only want this rule to fire for enrollments not in SPED schools. */
    and exists (
            select 1
            from  enrollments_minus_sped_sch_enroll x
            where se.k_student = x.k_student
                and se.k_lea = x.k_lea
    )
)
select *
from errors 