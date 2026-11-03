{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_student_education_organization_association_address set not null",
        "alter table {{ this }} add primary key (k_student_education_organization_association_address, address_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_address foreign key (k_student_education_organization_association_address) 
            references {{ ref('fct_student_education_organization_association_address') }}"
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
    select 
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
        addr.address_begin_date,
        addr.address_end_date
    from student_education_organization_assoc seoa
    join addresses addr
        on addr.k_student = seoa.k_student
        and coalesce(addr.k_lea, 'null_lea') = coalesce(seoa.k_lea, 'null_lea')
        and coalesce(addr.k_school, 'null_school') = coalesce(seoa.k_school, 'null_school')
)
select * from formatted