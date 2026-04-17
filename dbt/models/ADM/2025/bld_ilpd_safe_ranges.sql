{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap any ILPD records that exist.
Hopefully, in the future, we won't have any because the data will get cleaned up, but we have it right now.
*/

with ilpd_statuses as (
    select ilpd.k_student, ilpd.k_school, ilpd.school_year, ilpd.tenant_code, ilpd.api_year,
        ilpd.student_unique_id, ilpd.ed_org_id,
        exploded_program.value:serviceBeginDate::date as service_begin_date,
        coalesce(exploded_program.value:serviceEndDate::date, to_date(concat(ilpd.school_year, '-06-30'), 'yyyy-MM-dd')) as service_end_date
    from {{ ref('stg_ef3__student_language_instruction_program_associations') }} ilpd,
    lateral variant_explode(ilpd.v_language_instruction_program_services) as exploded_program
    where ilpd.program_name = 'ILPD'
        and ilpd.k_lea is null
),
ordered as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        service_begin_date, service_end_date,
        max(service_end_date) over (
            partition by k_student, k_school, school_year, tenant_code
            order by service_begin_date, service_end_date
            rows between unbounded preceding and 1 preceding
        ) as prev_end_date
    from ilpd_statuses
),
islands as (
    select *,
        case
            -- gap of at least 1 day = new island
            when service_begin_date > date_add(prev_end_date, 1) then 1
            else 0
        end as is_new_island
    from ordered
), 
island_ids as (
    select *,
        sum(is_new_island) over (
            partition by k_student, k_school, school_year, tenant_code
            order by service_begin_date, service_end_date
            rows between unbounded preceding and current row
        ) as island_id
    from islands
),
aggregated as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        min(service_begin_date) as service_begin_date,
        max(service_end_date) as service_end_date
    from island_ids
    group by k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id, island_id
),
final_with_next as (
    select *,
        lead(service_begin_date) over (
            partition by k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id
            order by service_begin_date
        ) as next_island_begin_date
    from aggregated
)
select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
    service_begin_date,
    case
        when next_island_begin_date is not null and next_island_begin_date <= service_end_date
             then date_sub(next_island_begin_date, 1)
        else service_end_date
    end as service_end_date
from final_with_next