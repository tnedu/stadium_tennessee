{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3001 %}

/* Students with the IMMG student_characteristic must have Date Entered US populated. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    where k_lea is not null
        {{ school_year_exists(error_code, 'seoa') }}
),
primary_enrollments (
    select ssa.k_student, ssa.school_year, s.k_lea, ssa.school_id, ssa.entry_date, ssa.exit_withdraw_date
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join {{ ref('stg_ef3__schools') }} s
        on s.k_school = ssa.k_school
    where ssa.is_primary_school = true
        and (ssa.exit_withdraw_date is null
            or ssa.entry_date < ssa.exit_withdraw_date) 
),
students_w_errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        stu.state_student_id, 
        pe.school_id, pe.entry_date, pe.exit_withdraw_date
    from stg_student_edorgs se
    join {{ ref('stg_ef3__students') }} stu
        on se.k_student = stu.k_student
    left join primary_enrollments pe
        on pe.k_student = se.k_student
        and pe.k_lea = se.k_lea
        and pe.school_year = se.school_year
    where stu.date_entered_us is null
        and exists (
                select 1
                from {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
                where sc.k_lea = se.k_lea
                    and sc.k_student = se.k_student
                    and sc.student_characteristic = 'IMMIG'
            )
    qualify 1 = 
        dense_rank() over(
            partition by se.k_student, se.school_year
            order by 
                -- case when pe.entry_date is not null then 1 else 2 end,
                pe.entry_date desc,
                pe.exit_withdraw_date desc nulls first
            )
)
select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
se.state_student_id as legacy_state_student_id,
{{ error_code }} as error_code,
concat('Immigrant Student ', 
se.student_unique_id, ' (', coalesce(se.state_student_id, '[no value]'), ') ',
'requires Date Entered US on District level Student/EdOrg Association.') as error,
{{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
rules.tdoe_severity
from students_w_errors se
join {{ ref('business_rules_year_ranges') }} rules
on rules.tdoe_error_code = {{ error_code }}