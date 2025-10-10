{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3008 %}

/* brule as per standard */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
/* Student School Association with valid enrollments */
stg_student_school_associations as (
    select *,
            {{ get_district_from_school_id('ssa.school_id') }}  as ed_org_id
    from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    where exists (
        select 1
        from brule
        where cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
    and (ssa.exit_withdraw_date is null
    or ssa.entry_date < ssa.exit_withdraw_date)
)
/* Student Enrollments that does not exist in Student/EdOrg Associations. */
select distinct ssa.k_student, lea.k_lea, ssa.k_school, ssa.school_year, ssa.ed_org_id, s.student_unique_id,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
       concat('Immigrant Student ', 
        s.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'requires Student/EdOrg Associations on District level when Student School Association exists.') as error,
    brule.tdoe_severity as severity
from stg_student_school_associations ssa
join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
    on ssa.k_student = s.k_student
join {{ ref('edu_edfi_source', 'stg_ef3__local_education_agencies') }} lea
    on ssa.ed_org_id = lea.lea_id
join brule
    on cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
where not exists (
            select 1 from {{ ref('stg_ef3__student_education_organization_associations_orig') }} se
            where se.k_student = ssa.k_student
            and se.school_year = ssa.school_year
            and se.ed_org_id = ssa.ed_org_id
        )