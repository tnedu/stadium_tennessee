{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap Expulsion records that exist.
*/

with expulsions as (
    select distinct k_student, k_school, school_year, tenant_code, discipline_action,
        discipline_date_begin, 
        coalesce(discipline_date_end, to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd')) as discipline_date_end   
    from {{ ref('wrk_expulsion_windows') }}
    where discipline_date_begin is not null
),
clean_expulsions as (
    select k_student, k_school, school_year, tenant_code, discipline_action, discipline_date_begin, discipline_date_end,
        lead(discipline_date_begin) over (
            partition by k_student, k_school, school_year, tenant_code, discipline_action
            order by discipline_date_begin, discipline_date_end) as next_discipline_begin_date
    from expulsions 
),
safe_dates as (
    select k_student, k_school, school_year, tenant_code, discipline_action, discipline_date_begin, discipline_date_end,
        case
            when next_discipline_begin_date is not null and next_discipline_begin_date < discipline_date_end then date_sub(next_discipline_begin_date, 1)
            else discipline_date_end
        end as safe_discipline_date_end
    from clean_expulsions
)
select *
from safe_dates
where discipline_date_begin <= safe_discipline_date_end