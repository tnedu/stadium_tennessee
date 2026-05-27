{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3300 %}

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
),
ssa_ssd as (
    select 
        ssas.*,
        sd.col.effectiveDate::date as ssd_date_start
    from ssas
    lateral view outer explode(studentStandardDays) sd
),
errors as (
    /* Student Standard Day events must be within enrollment period. */
    select 
        ssd.k_student, 
        ssd.k_school, 
        ssd.k_school_calendar,
        ssd.school_id,
        ssd.student_unique_id,
        ssd.school_year,
        ssd.entry_date,
        ssd.entry_grade_level,
        ssd.calendar_code,
        s.state_student_id as legacy_state_student_id,
        brule.tdoe_error_code as error_code,
        concat('Student Standard Day for Student ', 
            ssd.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'does not fall within Enrollment Period. Enrollment Start Date: ',
            ifnull(ssd.entry_date, '[null]'), ', Enrollment End Date: ', 
            ifnull(ssd.exit_withdraw_date, '[null]'), ', Student Standard Day Effective Date: ', 
            ssd.ssd_date_start, '.') as error
    from ssa_ssd ssd
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssd.k_student
    join brule
        on ssd.school_year between brule.error_school_year_start and brule.error_school_year_end
    where (
        not(ssd.ssd_date_start between ssd.entry_date 
            and ifnull(ssd.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd')))
        )
)
select errors.*,
    {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
    brule.tdoe_severity
from errors errors
join brule
    on errors.school_year between brule.error_school_year_start and brule.error_school_year_end