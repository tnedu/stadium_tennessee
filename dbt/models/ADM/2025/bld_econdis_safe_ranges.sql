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
                and x.student_characteristic = stu_chars.student_characteristic
        )
),
ordered as (
    select tenant_code, school_year, k_student, k_lea, begin_date, end_date,
        lag(end_date) over (
            partition by tenant_code, school_year, k_student, k_lea
            order by begin_date, end_date
        ) as prev_end_date
    from econ_disadvantaged
),
islands as (
    select *,
        case
            when prev_end_date is null then 1
            -- gap of at least 1 day = new island
            when begin_date > date_add(prev_end_date, 1) then 1
            else 0
        end as is_new_island
    from ordered
), 
island_ids as (
    select *,
        sum(is_new_island) over (
            partition by tenant_code, school_year, k_student, k_lea
            order by begin_date, end_date
            rows between unbounded preceding and current row
        ) as island_id
    from islands
)
select tenant_code, school_year, k_student, k_lea,
    min(begin_date) as begin_date,
    case
        when max(end_date) = to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd')
            then max(end_date)
        else date_sub(max(end_date), 1)
    end as end_date
from island_ids
group by tenant_code, school_year, k_student, k_lea, island_id