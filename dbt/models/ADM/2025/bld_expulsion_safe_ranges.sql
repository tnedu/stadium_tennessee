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
    select distinct k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action,
        discipline_date_begin, 
        coalesce(discipline_date_end, to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd')) as discipline_date_end   
    from {{ ref('wrk_expulsion_windows') }}
    where discipline_date_begin is not null
        and discipline_action = 'E'
),
ordered as (
    select k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action,
        discipline_date_begin, discipline_date_end,
        max(discipline_date_end) over (
            partition by k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action
            order by discipline_date_begin, discipline_date_end
            rows between unbounded preceding and 1 preceding
        ) as prev_end_date
    from expulsions
),
islands as (
    select *,
        case
            when prev_end_date is null then 1
            -- gap of at least 1 day = new island
            when discipline_date_begin > date_add(prev_end_date, 1) then 1
            else 0
        end as is_new_island
    from ordered
), 
island_ids as (
    select *,
        sum(is_new_island) over (
            partition by k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action
            order by discipline_date_begin, discipline_date_end
            rows between unbounded preceding and current row
        ) as island_id
    from islands
),
aggregated as (
    select k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action,
        min(discipline_date_begin) as discipline_date_begin,
        max(discipline_date_end) as discipline_date_end
    from island_ids
    group by k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action, island_id
),
final_with_next as (
    select *,
        lead(discipline_date_begin) over (
            partition by k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action
            order by discipline_date_begin
        ) as next_island_begin_date
    from aggregated
)
select k_student, k_school, k_school_calendar, school_year, tenant_code, discipline_action,
    discipline_date_begin,
    case
        when next_island_begin_date is not null and next_island_begin_date <= discipline_date_end
             then date_sub(next_island_begin_date, 1)
        else discipline_date_end
    end as discipline_date_end
from final_with_next