{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3303 %}

/* A Student's SSD duration must be non-zero. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity,
        rule_model
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
    and rule_model = '{{this.identifier}}'
),
ssas as (
    select 
        ssa.k_student, ssa.k_school, ssa.k_school_calendar, cast(ssa.school_id as int) as school_id,
        ssa.student_unique_id, cast(ssa.school_year as int) as school_year, ssa.entry_date, 
        ssa.exit_withdraw_date, ssa.entry_grade_level, ssa.calendar_code
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join brule brule
        on cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    /* Valid enrollments only. We have to edit this once the zero-day early grads goes to prod. */
    where exists (
        select 1
        from {{ ref('valid_enrollments') }} ve
        where ve.k_student = ssa.k_student
            and ve.k_school = ssa.k_school
            and ve.k_school_calendar = ssa.k_school_calendar
            /* to add when zero-day early grads goes to prod. */
            /*and ve.is_zeroday_early_graduate = 0 */
        )
),
ssa_ssd as (
    select 
        ssas.*,
        sd.col.effectiveDate::date as ssd_date_start,
        sd.col.studentStandardDayDuration::int as ssd_duration
    from ssas
    lateral view outer explode(studentStandardDays) sd
),
errors as (
    select 
        ssa.k_student, 
        ssa.k_school, 
        ssa.k_school_calendar,
        ssa.school_id, 
        ssa.student_unique_id,
        ssa.school_year,
        ssa.entry_date,
        ssa.entry_grade_level,
        ssa.calendar_code,
        s.state_student_id as legacy_state_student_id,
        brule.tdoe_error_code as error_code,
        concat('SSD Duration missing for Student: ', ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]') ,'), ',
            'District: ', {{ get_district_from_school_id('ssa.school_id') }}, ', ',
            'School: ', ssa.school_id, ', ',
            'Enrollment Entry Date: ', ssa.entry_date, ', ',
            'Enrollment End Date: ', coalesce(ssa.exit_withdraw_date, '[null]'), '.') as error
    from ssa_ssd ssa
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    join brule
        on ssa.school_year between brule.error_school_year_start and brule.error_school_year_end
    where coalesce(ssa.ssd_duration, 0) = 0
)
select errors.*,
    {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
    brule.tdoe_severity
from errors errors
join brule
    on errors.school_year between brule.error_school_year_start and brule.error_school_year_end