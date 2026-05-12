{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_staff set not null",
        "alter table {{ this }} alter column ed_org_id set not null",
        "alter table {{ this }} alter column contact_title set not null",
        "alter table {{ this }} alter column email_address set not null",
        "alter table {{ this }} add primary key (k_staff, ed_org_id, contact_title, email_address)",
    ]
  )
}}

{{ edu_wh.cds_depends_on('tdoe:staff_contact_association:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:staff_contact_association:custom_data_sources', []) %}

with stg_staff_edorg_contacts as (
    select * from {{ ref('stg_ef3__staff_education_organization_contact_associations') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
formatted as (
    select 
        dim_staff.k_staff,
        stg_staff_edorg_contacts.k_lea,
        stg_staff_edorg_contacts.k_school,
        stg_staff_edorg_contacts.tenant_code,
        stg_staff_edorg_contacts.school_year,
        stg_staff_edorg_contacts.ed_org_id,
        stg_staff_edorg_contacts.contact_title,
        stg_staff_edorg_contacts.contact_type,
        stg_staff_edorg_contacts.email_address

        -- custom data sources columns
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_staff_edorg_contacts stg_staff_edorg_contacts
    join dim_staff dim_staff
        on stg_staff_edorg_contacts.k_staff = dim_staff.k_staff

    -- custom data sources
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted