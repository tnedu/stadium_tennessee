{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3104 %}

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
    select *,
        brule.tdoe_error_code,
        brule.tdoe_severity
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join brule
        on cast(ssa.school_year as int)
           between brule.error_school_year_start and brule.error_school_year_end
),
primary_enroll_ordered as (
    select student_unique_id, school_year, entry_date,
        ifnull(exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd')) as exit_date,
        max(ifnull(exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd'))) over (
            partition by student_unique_id, school_year
            order by entry_date, exit_withdraw_date
            rows between unbounded preceding and 1 preceding) as prev_max_exit
    from stg_student_school_associations
    where is_primary_school = true
),
primary_islands as (
    /* New island starts when there is a gap of 1+ days after all prior primary enrollments */
    select *,
        sum(case when prev_max_exit is null
                   or date_sub(entry_date, 1) > prev_max_exit
                 then 1 else 0 end) over (
            partition by student_unique_id, school_year
            order by entry_date, exit_date
            rows between unbounded preceding and current row) as prim_enroll_rank_id
    from primary_enroll_ordered
),
primary_enrollments as (
    select student_unique_id, school_year, prim_enroll_rank_id,
        min(entry_date) as p_entry_date,
        max(exit_date) as p_exit_withdraw_date
    from primary_islands
    group by student_unique_id, school_year, prim_enroll_rank_id
),
errors as (
    /* Secondary enrollments require an open Primary enrollment. */
    select s.k_student, s.k_school, s.k_school_calendar, s.school_id, s.student_unique_id, s.school_year, 
        s.entry_date, s.entry_grade_level, s.entry_type,
        s2.state_student_id as legacy_state_student_id,
        s.tdoe_error_code as error_code,
        concat('No Active Primary Enrollment designated for Student ', 
            s.student_unique_id, ' (', coalesce(s2.state_student_id, '[no value]'), ')',
            '. Student Service School: ', s.school_id) as error,
        {{ severity_to_severity_code_case_clause('s.tdoe_severity') }},
        s.tdoe_severity
    from stg_student_school_associations s
    join {{ ref('stg_ef3__students') }} s2
        on s2.k_student = s.k_student
    where s.is_primary_school = false
        and not exists (
            select 1
            from primary_enrollments p
            where p.school_year = s.school_year
                and p.student_unique_id = s.student_unique_id
                and s.entry_date >= p.p_entry_date
                and ifnull(s.exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd')) <=
                    ifnull(p.p_exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd'))
        )
)
select *
from errors