{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3102 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
), 
errors as (
    /* Withdrawal Date must be within the school year begin and end date. */
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
        ssa.entry_date, ssa.entry_grade_level, ssa.entry_type,
        s.state_student_id as legacy_state_student_id,
        {{ error_code }} as error_code,
        concat('Student School Association Exit Withdrawal Date for Student ', 
            ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'does not fall within the school year. Value Received: ', ssa.exit_withdraw_date, 
            '. The state school year starts ',
            concat((ssa.school_year-1), '-07-01'), ' and ends ', concat(ssa.school_year, '-06-30'), '.') as error
    from stg_student_school_associations ssa
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    where ssa.exit_withdraw_date is not null
        and not(ssa.exit_withdraw_date between to_date(concat((ssa.school_year-1), '-07-01'), 'yyyy-MM-dd') 
            and to_date(concat(ssa.school_year, '-06-30'), 'yyyy-MM-dd'))
    )
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}