{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3009 %}

/* brule as per standard */
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
    from {{ ref('stg_ef3__student_education_organization_associations_orig') }} seoa
    where k_lea is null
        and exists (
        select 1
        from brule
        where cast(seoa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)

/* Student/EdOrg Associations that are on school level */
select distinct se.k_student, se.k_lea, cast( null as int ) as k_school, se.school_year,
            {{ get_district_from_school_id('se.ed_org_id') }}  as ed_org_id, se.student_unique_id,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Student ', 
        se.student_unique_id, '(', coalesce(s.state_student_id, '[no value]'), ') ',
        'requires Student/EdOrg Association on District level but exists at school level.') as error,
    brule.tdoe_severity as severity
from stg_student_edorgs se
join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
    on se.k_student = s.k_student
join brule
    on se.school_year between brule.error_school_year_start and brule.error_school_year_end
