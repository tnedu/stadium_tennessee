{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3103 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
),
errors as (
    /* Withdrawal Date must be greater than or equal to entry date. */
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
        ssa.entry_date, ssa.entry_grade_level,ssa.entry_type,
        s.state_student_id as legacy_state_student_id,
        {{ error_code }} as error_code,
        concat('Exit Withdrawal Date for Student ', 
            ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'must be greater than or equal to the Entry Date. Exit Withdrawal Date received: ',
            ssa.exit_withdraw_date, ', Entry Date: ', ssa.entry_date, '.') as error
    from stg_student_school_associations ssa
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    where ssa.exit_withdraw_date is not null
        and ssa.exit_withdraw_date < ssa.entry_date
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}