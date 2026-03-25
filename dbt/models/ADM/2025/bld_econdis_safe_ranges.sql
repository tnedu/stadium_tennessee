{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap Economically Disadvantaged records that exist.
*/

with econ_disadvantaged as (
    select distinct stu_chars.tenant_code, stu_chars.school_year, stu_chars.k_student, stu_chars.k_lea, 
        stu_chars.begin_date, 
        coalesce(stu_chars.end_date, to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd')) as end_date
    from {{ ref('fct_student_characteristics') }} stu_chars
    where exists (
            select 1
            from {{ ref('student_characteristics_attributes') }} x
            where stu_chars.school_year >= x.stuchar_school_year_start
                and (x.stuchar_school_year_end is null or stu_chars.school_year <= x.stuchar_school_year_end)
                and x.is_econ_disadvantaged = 'Y'
        )
),
clean_econ_disadvantaged as (
    select tenant_code, school_year, k_student, k_lea, begin_date, end_date,
        lead(begin_date) over (
            partition by tenant_code, school_year, k_student, k_lea
            order by begin_date, end_date) as next_begin_date
    from econ_disadvantaged 
),
safe_dates as (
    select tenant_code, school_year, k_student, k_lea, begin_date, end_date,
        case
            when next_begin_date is not null and next_begin_date < end_date then date_sub(next_begin_date, 1)
            else end_date
        end as safe_end_date
    from clean_econ_disadvantaged
)
select *
from safe_dates
where begin_date <= safe_end_date