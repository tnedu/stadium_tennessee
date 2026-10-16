{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3301 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_attendance as (
    select ssae.k_student, ssae.k_school, ssae.k_session, cast(ssae.school_year as int) as school_year, 
        ssae.school_id, ssae.student_unique_id, ssae.attendance_event_date, ssae.attendance_event_category,
        brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__student_school_attendance_events') }} ssae
    join brule
        on cast(ssae.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where ssae.attendance_event_category != 'Student Standard Day'
        /* Ignore service schools for this rule. */
        and not exists (
            select 1
            from {{ ref('service_schools') }} ss
            where ssd.k_school = ss.k_school
        ) 
),
errors as (
    /* Student Attendance/Absence events must be within enrollment period. */
    select ssd.k_student, ssd.k_school, ssd.k_session, ssd.school_year, cast(ssd.school_id as int) as school_id, ssd.student_unique_id,
        ssd.attendance_event_date, ssd.attendance_event_category,
        s.state_student_id as legacy_state_student_id,
        ssd.tdoe_error_code as error_code,
        concat('Student attendance/absence does not fall within Enrollment Period for Student ', 
            ssd.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ')',
            '. Enrollment Start Date: ',
            ifnull(ssa.entry_date, '[null]'), ', Enrollment End Date: ', 
            ifnull(ssa.exit_withdraw_date, '[null]'), ', Attendance Date: ', 
            ssd.attendance_event_date, '.') as error,
            {{ severity_to_severity_code_case_clause('ssd.tdoe_severity') }},
            ssd.tdoe_severity
    from stg_attendance ssd
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssd.k_student
    left outer join {{ ref('stg_ef3__student_school_associations') }} ssa
        on ssa.k_student = ssd.k_student
        and ssa.k_school = ssd.k_school
        and ssa.school_year = cast(ssd.school_year as int)
        /* No shows don't count. */
        and ssa.entry_date < ifnull(ssa.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
    where (
            ssa.k_student is null
            or (ssa.k_student is not null 
                and not(ssd.attendance_event_date between ssa.entry_date 
                    and ifnull(ssa.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd')))
                )
        )
        /* The date has to fit between some enrollment period even if it doesn't fit between EVERY enrollment 
            period for a student. */
        and not exists (
            select 1
            from {{ ref('stg_ef3__student_school_associations') }} x
            where x.k_student = ssd.k_student
                and x.k_school = ssd.k_school
                and x.school_year = cast(ssd.school_year as int)
                /* No shows don't count. */
                and x.entry_date < ifnull(x.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
                and ssd.attendance_event_date between x.entry_date 
                and ifnull(x.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
        )
)
select *
from errors
