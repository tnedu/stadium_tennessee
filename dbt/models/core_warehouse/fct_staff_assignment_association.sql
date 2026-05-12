{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_staff set not null",
        "alter table {{ this }} alter column ed_org_id set not null",
        "alter table {{ this }} alter column begin_date set not null",
        "alter table {{ this }} alter column staff_classification set not null",
        "alter table {{ this }} add primary key (k_staff, ed_org_id, begin_date, staff_classification)",
    ]
  )
}}

{{ edu_wh.cds_depends_on('tdoe:staff_assignment_association:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:staff_assignment_association:custom_data_sources', []) %}

with stg_staff_edorg_assignments as (
    select *,
        staff_ed_org_employment_reference:staffUniqueId::string as staff_edorg_employment_reference_staff_unique_id,
        staff_ed_org_employment_reference:educationOrganizationId::int as staff_edorg_employment_reference_ed_org_id,
        staff_ed_org_employment_reference:hireDate::date as staff_edorg_employment_reference_hire_date,
        {{ edu_edfi_source.extract_descriptor('staff_ed_org_employment_reference:employmentStatusDescriptor::string') }} as staff_edorg_employment_reference_employment_status
    from {{ ref('stg_ef3__staff_education_organization_assignment_associations') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
formatted as (
    select 
        dim_staff.k_staff,
        stg_staff_edorg_assignments.k_lea,
        stg_staff_edorg_assignments.k_school,
        stg_staff_edorg_assignments.ed_org_id,
        stg_staff_edorg_assignments.tenant_code,
        stg_staff_edorg_assignments.school_year,
        case
            when stg_staff_edorg_assignments.staff_ed_org_employment_reference is null then null
            else 
                {{ dbt_utils.generate_surrogate_key(
                    ['stg_staff_edorg_assignments.tenant_code',
                    'stg_staff_edorg_assignments.school_year',
                    'stg_staff_edorg_assignments.staff_edorg_employment_reference_staff_unique_id',
                    'stg_staff_edorg_assignments.staff_edorg_employment_reference_ed_org_id',
                    'stg_staff_edorg_assignments.staff_edorg_employment_reference_hire_date',
                    'stg_staff_edorg_assignments.staff_edorg_employment_reference_employment_status' ]
                ) }}
        end as k_staff_employment_association,
        stg_staff_edorg_assignments.position_title,
        stg_staff_edorg_assignments.begin_date,
        stg_staff_edorg_assignments.end_date,
        stg_staff_edorg_assignments.full_time_equivalency,
        stg_staff_edorg_assignments.order_of_assignment,
        stg_staff_edorg_assignments.staff_classification,
        stg_staff_edorg_assignments.credential_identifier,
        stg_staff_edorg_assignments.credential_state

        -- custom data sources columns
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_staff_edorg_assignments stg_staff_edorg_assignments
    join dim_staff dim_staff
        on stg_staff_edorg_assignments.k_staff = dim_staff.k_staff

    -- custom data sources
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted

