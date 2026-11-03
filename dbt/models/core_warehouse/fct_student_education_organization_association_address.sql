{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_student_education_organization_association set not null",
        "alter table {{ this }} alter column k_student_education_organization_association_address set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} add primary key (k_student_education_organization_association_address)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('edu_wh', 'dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('edu_wh', 'dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('edu_wh', 'dim_school') }}"
    ]
  )
}}

{{ edu_wh.cds_depends_on('tdoe:student_standard_day:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:student_standard_day:custom_data_sources', []) %}

/*sae student standard day for prior years*/
with student_education_organization_assoc as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations') }} 
),
addresses as (
    select *
    from {{ ref('stg_ef3__stu_ed_org__addresses') }}
),
formatted as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(
            [
                'seoa.tenant_code',
                'seoa.api_year',
                'seoa.ed_org_id',
                'seoa.student_unique_id'
            ]
        )}} as k_student_education_organization_association,
        {{ dbt_utils.generate_surrogate_key(
            [
                'seoa.tenant_code',
                'seoa.api_year',
                'seoa.ed_org_id',
                'seoa.student_unique_id',
                'addr.address_type',
                'addr.state_code',
                'addr.city',
                'addr.postal_code',
                'addr.street_address'
            ]
        )}} as k_student_education_organization_association_address,
        seoa.k_student,
        seoa.k_student_xyear,
        seoa.k_lea,
        seoa.k_school,
        seoa.tenant_code,
        seoa.school_year,
        seoa.ed_org_id,
        seoa.ed_org_type,
        addr.address_type,
        addr.street_address,
        addr.apartment_room_suite_number,
        addr.city,
        addr.state_code,
        addr.postal_code,
        addr.name_of_county,
        addr.building_site_number,
        addr.locale,
        addr.congressional_district,
        addr.county_fips_code,
        addr.do_not_publish,
        addr.latitude,
        addr.longitude,
        addr.full_address
    from student_education_organization_assoc seoa
    join addresses addr
        on addr.k_student = seoa.k_student
        and coalesce(addr.k_lea, 'null_lea') = coalesce(seoa.k_lea, 'null_lea')
        and coalesce(addr.k_school, 'null_school') = coalesce(seoa.k_school, 'null_school')
)
select * from formatted