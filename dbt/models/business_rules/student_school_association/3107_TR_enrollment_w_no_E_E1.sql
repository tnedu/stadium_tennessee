{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3107 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
),
errors as (
    /* If TR enrollment exists then an E or E1 enrollment should exist prior, at a different school. */
    select p1.k_student, p1.k_school, p1.k_school_calendar, p1.school_id, p1.student_unique_id, p1.school_year, 
        p1.entry_date, p1.entry_grade_level, p1.entry_type,
        s.state_student_id as legacy_state_student_id,
        {{ error_code }} as error_code,
        concat('Student ', 
            p1.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'has TR Enrollment but no corresponding E or E1 Enrollment at a different school. Enrollment Begin Date: ', p1.entry_date,
            ', Enrollment End Date: ', ifnull(p1.exit_withdraw_date, '[null]'), ', Enrollment Reason Code: ', p1.entry_type) as error
    from stg_student_school_associations p1
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = p1.k_student
    where p1.is_primary_school = true
        and p1.entry_type in ('TR')
        and not exists (
            select 1
            from stg_student_school_associations p2
            where p2.is_primary_school = true
                and p2.entry_type in ('E','E1')
                and p2.school_year = p1.school_year
                and p2.student_unique_id = p1.student_unique_id
                and p2.school_id != p1.school_id
                and p2.entry_date < p1.entry_date
                and p2.exit_withdraw_date is not null
                and p2.exit_withdraw_date <= p1.entry_date
        )
    order by p1.school_year, p1.student_unique_id, p1.entry_date
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}