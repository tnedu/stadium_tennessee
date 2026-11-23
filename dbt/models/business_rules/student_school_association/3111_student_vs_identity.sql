{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}
{% set error_code = 3111 %}

with brule as (
    select tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
stg_student_school_associations as (
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year,
        ssa.entry_date, ssa.entry_grade_level, ssa.calendar_code, sch.lea_id,
        brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join {{ ref('stg_ef3__schools') }} sch
        on sch.k_school = ssa.k_school
    join brule
        on cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    qualify row_number() over (partition by ssa.k_student, sch.k_lea, ssa.school_year order by ssa.entry_date desc, ssa.school_id) = 1
),
identities as (
    select i.UniqueId as student_unique_id, i.FirstName as first_name, i.MiddleName as middle_name, i.LastName as last_name,
        to_date(i.DateOfBirth) as birth_date
    from {{ source('identity', 'identities') }} i
    where i.Role = 0 /* 0 = student */
),
errors as (
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year,
        ssa.entry_date, ssa.entry_grade_level, ssa.calendar_code,
        s.state_student_id as legacy_state_student_id,
        ssa.tdoe_error_code as error_code,
        concat('Student ', ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'demographic data submitted by District ', coalesce(cast(ssa.lea_id as string), '[no value]'), ' does not match Identity.\n',
            'Student resource: First: ', coalesce(s.first_name, '[no value]'), ', Middle: ', coalesce(s.middle_name, '[no value]'), ', Last: ', coalesce(s.last_name, '[no value]'), ', DOB: ', coalesce(cast(s.birth_date as string), '[no value]'), '\n',
            'Identity: First: ', coalesce(i.first_name, '[no value]'), ', Middle: ', coalesce(i.middle_name, '[no value]'), ', Last: ', coalesce(i.last_name, '[no value]'), ', DOB: ', coalesce(cast(i.birth_date as string), '[no value]')) as error,
        {{ severity_to_severity_code_case_clause('ssa.tdoe_severity') }},
        ssa.tdoe_severity
    from stg_student_school_associations ssa
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    join identities i
        on i.student_unique_id = ssa.student_unique_id
    where upper(trim(coalesce(s.first_name, ''))) <> upper(trim(coalesce(i.first_name, '')))
        or upper(trim(coalesce(s.middle_name, ''))) <> upper(trim(coalesce(i.middle_name, '')))
        or upper(trim(coalesce(s.last_name, ''))) <> upper(trim(coalesce(i.last_name, '')))
        or not (s.birth_date <=> i.birth_date)
)
select *
from errors