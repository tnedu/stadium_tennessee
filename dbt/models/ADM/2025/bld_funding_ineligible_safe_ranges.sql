{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap Funding Ineligible records that exist.
*/

with funding_ineligible as (
    select distinct stu_chars.tenant_code, stu_chars.school_year, stu_chars.k_student, stu_chars.k_lea, 
        stu_chars.begin_date, 
        coalesce(stu_chars.end_date, to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd')) as end_date
    from {{ ref('fct_student_characteristics') }} stu_chars
    where exists (
            select 1
            from {{ ref('student_characteristics_attributes') }} x
            where stu_chars.school_year >= x.stuchar_school_year_start
                and (x.stuchar_school_year_end is null or stu_chars.school_year <= x.stuchar_school_year_end)
                and x.is_funding_ineligible = 'Y'
                and x.student_characteristic = stu_chars.student_characteristic
        )
),
ordered as (
    select tenant_code, school_year, k_student, k_lea, begin_date, end_date,
        max(end_date) over (
            partition by tenant_code, school_year, k_student, k_lea
            order by begin_date, end_date
            rows between unbounded preceding and 1 preceding
        ) as prev_end_date
    from funding_ineligible
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
),
aggregated as (
    select tenant_code, school_year, k_student, k_lea,
        min(begin_date) as begin_date,
        max(end_date) as end_date
    from island_ids
    group by tenant_code, school_year, k_student, k_lea, island_id
),
final_with_next as (
    select *,
        lead(begin_date) over (
            partition by tenant_code, school_year, k_student, k_lea
            order by begin_date
        ) as next_island_begin_date
    from aggregated
)
select tenant_code, school_year, k_student, k_lea,
    begin_date,
    case
        when next_island_begin_date is not null and next_island_begin_date <= end_date
             then date_sub(next_island_begin_date, 1)
        else end_date
    end as end_date
from final_with_next
