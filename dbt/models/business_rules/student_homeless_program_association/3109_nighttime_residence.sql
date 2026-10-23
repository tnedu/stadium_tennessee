{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3109 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_homeless as (
    select sh.*,
            brule.tdoe_error_code,
            brule.tdoe_severity
    from {{ ref('stg_ef3__student_homeless_program_associations') }} sh
    join brule
        on cast(sh.school_year as int)
           between brule.error_school_year_start and brule.error_school_year_end
    where sh.k_lea is not null
),
enrollments_minus_sped_sch_enroll as (
    select ssa.k_student, school.k_lea, school.lea_id, ssa.k_school, ssa.school_id, ssa.k_school_calendar,
            ssa.tenant_code, ssa.api_year, ssa.student_unique_id, ssa.school_year, ssa.is_primary_school,
            ssa.entry_date, ssa.exit_withdraw_date, ssa.calendar_code
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join {{ ref('stg_ef3__schools') }} school
        on ssa.k_school = school.k_school
    /* we want to ignore service schools for this rule */
    where not exists (
        select 1
        from {{ ref('service_schools') }} ss
        where ssa.k_school = ss.k_school
    ) 
),
errors as (
    /* Homeless Students must have a Primary Nighttime Residence. */
    select h.k_student, h.k_program, h.k_lea, h.school_year, h.student_unique_id, h.ed_org_id, h.program_enroll_begin_date,
        s.state_student_id as legacy_state_student_id,
        h.tdoe_error_code as error_code,
        concat('Homeless Primary Nighttime Residence for Student ', 
            h.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'cannot be blank.') as error,
        {{ severity_to_severity_code_case_clause('h.tdoe_severity') }},
        h.tdoe_severity
    from stg_homeless h
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = h.k_student
    where h.homeless_primary_nighttime_residence is null
    /* We only want this rule to fire for enrollments not in SPED schools. */
    and exists (
            select 1
            from  enrollments_minus_sped_sch_enroll x
            where h.k_student = x.k_student
                and h.k_lea = x.k_lea
    )
)
select *
from errors