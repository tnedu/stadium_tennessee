{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
Student Expulsions has a start date and then a number of school days. But that's hard to reason out
when joining lots of other tables. So the purpose of this model is to figure out expulsion windows.
That is to say, a student is expelled from Date A to Date B, inclusive. That's much easier to use
when you need to know if they are expelled on any given day (for ADM calculations).
*/

with disciplines as (
    select *
    from {{ ref('fct_student_discipline_actions') }} fsda
    where fsda.discipline_action = 'E'
        and fsda.discipline_action_length is not null
        and coalesce(fsda.actual_discipline_action_length, fsda.discipline_action_length) > 0
),
school_days_by_school as (
    select distinct fssa.k_school, dcd.calendar_date
    from {{ ref('fct_student_school_association') }} fssa
    join {{ ref('dim_calendar_date') }} dcd
        on dcd.k_school_calendar = fssa.k_school_calendar
        and dcd.k_school = fssa.k_school
        and dcd.is_school_day = true
    where exists (
            select 1
            from disciplines d
            where d.k_student = fssa.k_student
                and d.k_school = fssa.k_school
        )
        and exists (
            select 1
            from {{ ref('valid_enrollments') }} ve
            where ve.k_student = fssa.k_student
                and ve.k_school = fssa.k_school
                and ve.k_school_calendar = fssa.k_school_calendar
                and ve.entry_date = fssa.entry_date
                and ve.is_primary_school = fssa.is_primary_school
        )
)
select k_student, k_school, school_year, tenant_code, discipline_date, discipline_action_length,
    min(calendar_date) as discipline_date_begin,
    max(calendar_date) as discipline_date_end
from (
    select d.k_student, d.k_school, d.school_year, d.tenant_code, d.discipline_date, 
        coalesce(d.actual_discipline_action_length, d.discipline_action_length) as discipline_action_length,
        sd.calendar_date,
        row_number() over (
            partition by d.k_student, d.k_school, d.school_year, d.tenant_code, d.discipline_date
            order by sd.calendar_date) as rn
    from disciplines d
    join school_days_by_school sd
        on sd.k_school = d.k_school
    where sd.calendar_date >= d.discipline_date
)
where rn <= discipline_action_length
group by k_student, k_school, school_year, tenant_code, discipline_date, discipline_action_length