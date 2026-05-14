{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3110 %}

with brule as (
    select 
        tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_school_associations as (
    select *
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    where exists (
        select 1
        from brule
        where cast(ssa.school_year as int)
            between brule.error_school_year_start and brule.error_school_year_end
    )
),
valid_enrollents_minus_zeroday_early_grads as (
    select *
    from {{ ref('valid_enrollments') }}
    where is_zeroday_early_graduate = 0
),
errors as (
    /* Calendar code missing on Student School Association record */
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
        concat(
            'Calendar code is missing for Student ',
            ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'at School ID ', ssa.school_id,
            ', Entry Date: ', ssa.entry_date
        ) as error
    from stg_student_school_associations ssa
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    join brule
        on ssa.school_year between brule.error_school_year_start and brule.error_school_year_end
    where 
        (ssa.calendar_code is null or trim(ssa.calendar_code) = '')
        /* We only want this rule to fire if there exists an enrollment that is non-zero-day early grad. */
        and exists (
            select 1
            from valid_enrollents_minus_zeroday_early_grads x
            where ssa.k_student = x.k_student
                and ssa.k_school = x.k_school
                and ssa.k_school_calendar = x.k_school_calendar
                and ssa.entry_date = x.entry_date
                and ssa.is_primary_school = x.is_primary_school
        )
    order by ssa.school_year, ssa.student_unique_id, ssa.entry_date
)

select 
    errors.*,
    {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
    brule.tdoe_severity
from errors
join brule
    on errors.school_year between brule.error_school_year_start and brule.error_school_year_end