{{
  config(
    materialized="table",
    schema="stg_validations",
    enabled=var('edu:data_validation', {}).get('enabled', false)
  )
}}
{% set error_code = 9999 %}
with validations as (
    select k_school,
        {{ edu_edfi_source.validation_struct(
            severity="'warning'",
            code=error_code,
            message="concat('This is a fake error for a school.')"
        ) }} as validation
    from {{ ref('dim_school') }}
    where k_school = '448344b7628e7c532523d38c733dfa32'
)
select *
from validations