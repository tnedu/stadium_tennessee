{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap any SPED records that exist.
*/

with sped_options as (
    select sped.k_student, sped.k_lea, sped.school_year, sped.tenant_code, sped.api_year,
        sped.student_unique_id, sped.ed_org_id, 
        {{ edu_edfi_source.extract_descriptor('exploded_services.value:specialEducationProgramServiceDescriptor::string') }} as participation_status,
        cast(regexp_extract({{ edu_edfi_source.extract_descriptor('exploded_services.value:specialEducationProgramServiceDescriptor::string') }}, 'Option (\\d+)', 1) as int) as option,
        exploded_services.value:primaryIndicator::boolean as primary_indicator,
        exploded_services.value:serviceBeginDate::date as service_begin_date,
        coalesce(exploded_services.value:serviceEndDate::date, to_date(concat(sped.school_year, '-06-30'), 'yyyy-MM-dd')) as service_end_date,
        sped.v_ext:tdoe:serviceEligibilityDate::date as service_eligibility_date
    from {{ ref('stg_ef3__student_special_education_program_associations') }} sped,
    lateral variant_explode(sped.v_special_education_program_services) as exploded_services
    where sped.k_school is null
        and sped.program_name = 'Special Education'
),
rank_sped_options as (
    select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, option,
        primary_indicator, service_begin_date, service_end_date, service_eligibility_date,
        row_number() over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator, service_begin_date
            order by option desc
        ) as rn
    from sped_options
    qualify rn = 1
),
ordered as (
    select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, option,
        primary_indicator, service_begin_date, service_end_date, service_eligibility_date,
        max(service_end_date) over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator
            order by service_begin_date, service_end_date
            rows between unbounded preceding and 1 preceding
        ) as prev_end_date,
        lag(participation_status) over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator
            order by service_begin_date, service_end_date
        ) as prev_participation_status

    from rank_sped_options
),
islands as (
    select *,
        case
            when prev_end_date is null then 1
            -- gap of at least 1 day = new island
            when service_begin_date > date_add(prev_end_date, 1) then 1
            when participation_status != prev_participation_status then 1
            else 0
        end as is_new_island
    from ordered
), 
island_ids as (
    select *,
        sum(is_new_island) over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator
            order by service_begin_date, service_end_date
            rows between unbounded preceding and current row
        ) as island_id
    from islands
),
aggregated as (
    select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, option, primary_indicator,
        max(service_eligibility_date) as service_eligibility_date,
        min(service_begin_date) as service_begin_date,
        max(service_end_date) as service_end_date
    from island_ids
    group by k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, option, primary_indicator, island_id
),
final_with_next as (
    select *,
        lead(service_begin_date) over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator
            order by service_begin_date, service_end_date
        ) as next_island_begin_date
    from aggregated
)
select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
    participation_status, option, primary_indicator, service_eligibility_date,
    service_begin_date,
    case
        when next_island_begin_date is not null and next_island_begin_date <= service_end_date
             then date_sub(next_island_begin_date, 1)
        else service_end_date
    end as service_end_date
from final_with_next