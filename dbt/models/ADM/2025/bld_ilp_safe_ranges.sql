{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap any ILP records that exist.
Hopefully, in the future, we won't have any because the data will get cleaned up, but we have it right now.
*/

with ilp_statuses as (
    select ilp.k_student, ilp.k_school, ilp.school_year, ilp.tenant_code, ilp.api_year,
        ilp.student_unique_id, ilp.ed_org_id,
        {{ edu_edfi_source.extract_descriptor('exploded_status.value:participationStatusDescriptor::string') }} as participation_status,
        exploded_status.value:statusBeginDate::date as status_begin_date,
        coalesce(exploded_status.value:statusEndDate::date, to_date(concat(ilp.school_year, '-06-30'), 'yyyy-MM-dd')) as status_end_date,
        ilp.v_ext:tdoe:totalYearsESL::int as total_years_esl
    from {{ ref('stg_ef3__student_language_instruction_program_associations') }} ilp,
    lateral variant_explode(ilp.v_program_participation_statuses) as exploded_status
    where ilp.program_name = 'ILP'
        and ilp.k_lea is null
),
rank_ilp_statuses as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status,
        status_begin_date,
        status_end_date,
        total_years_esl,
        row_number() over (
            partition by k_student, k_school, school_year, tenant_code, status_begin_date
            order by 
                case participation_status
                    when 'W' then 1
                    when 'L' then 2
                    when '1' then 3
                    when '2' then 4
                end
        ) as rn
    from ilp_statuses
    qualify rn = 1
),
ordered as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, status_begin_date, status_end_date, total_years_esl,
        max(status_end_date) over (
            partition by k_student, k_school, school_year, tenant_code
            order by status_begin_date, status_end_date
            rows between unbounded preceding and 1 preceding
        ) as prev_end_date,
        lag(participation_status) over (
            partition by k_student, k_school, school_year, tenant_code
            order by status_begin_date, status_end_date
        ) as prev_participation_status
    from rank_ilp_statuses
),
islands as (
    select *,
        case
            when prev_end_date is null then 1
            -- gap of at least 1 day = new island
            when status_begin_date > date_add(prev_end_date, 1) then 1
            when participation_status != prev_participation_status then 1
            else 0
        end as is_new_island
    from ordered
), 
island_ids as (
    select *,
        sum(is_new_island) over (
            partition by k_student, k_school, school_year, tenant_code
            order by status_begin_date, status_end_date
            rows between unbounded preceding and current row
        ) as island_id
    from islands
),
aggregated as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status,
        max(total_years_esl) as total_years_esl,
        min(status_begin_date) as status_begin_date,
        max(status_end_date) as status_end_date
    from island_ids
    group by k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id, 
        participation_status, island_id
),
final_with_next as (
    select *,
        lead(status_begin_date) over (
            partition by k_student, k_school, school_year, tenant_code
            order by status_begin_date
        ) as next_island_begin_date
    from aggregated
)
select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
    participation_status, total_years_esl,
    status_begin_date,
    case
        when next_island_begin_date is not null and next_island_begin_date <= status_end_date
             then date_sub(next_island_begin_date, 1)
        else status_end_date
    end as status_end_date,
    row_number() over (
        partition by k_student, k_school, school_year, tenant_code
        order by status_begin_date
    ) as seq
from final_with_next