{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_staff set not null",
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} alter column hire_date set not null",
        "alter table {{ this }} alter column employment_status set not null",
        "alter table {{ this }} add primary key (k_staff, k_lea, hire_date, employment_status)",
    ]
  )
}}

{{ edu_wh.cds_depends_on('tdoe:staff_employment_association:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:staff_employment_association:custom_data_sources', []) %}

with stg_staff_edorg_employment as (
    select * from {{ ref('stg_ef3__staff_education_organization_employment_associations') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
formatted as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['staff_edorg_employment.tenant_code',
            'staff_edorg_employment.school_year',
            'staff_edorg_employment.staff_unique_id',
            'staff_edorg_employment.ed_org_id',
            'staff_edorg_employment.hire_date',
            'staff_edorg_employment.employment_status']
        ) }} as k_staff_employment_association, 
        dim_staff.k_staff,
        coalesce(school.k_lea, staff_edorg_employment.k_lea) as k_lea,
        school.k_school,
        case
            when staff_edorg_employment.k_lea is null then 'School'
            else 'District'
        end as employed_by,
        staff_edorg_employment.tenant_code,
        staff_edorg_employment.school_year,
        staff_edorg_employment.employment_status,
        staff_edorg_employment.credential_identifier,
        staff_edorg_employment.credential_state,
        staff_edorg_employment.department,
        staff_edorg_employment.hire_date,
        staff_edorg_employment.end_date,
        staff_edorg_employment.full_time_equivalency,
        staff_edorg_employment.hourly_wage,
        staff_edorg_employment.annual_wage,
        staff_edorg_employment.offer_date,
        staff_edorg_employment.separation,
        staff_edorg_employment.separation_reason

        -- custom data sources columns
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_staff_edorg_employment staff_edorg_employment
    left join dim_school school
        on staff_edorg_employment.k_school = school.k_school
    join dim_staff dim_staff
        on staff_edorg_employment.k_staff = dim_staff.k_staff

    -- custom data sources
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted