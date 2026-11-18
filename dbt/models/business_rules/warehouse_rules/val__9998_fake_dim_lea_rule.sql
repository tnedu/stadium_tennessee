{{
  config(
    materialized="table",
    schema="stg_validations",
    enabled=var('edu:data_validation', {}).get('enabled', false)
  )
}}
{% set error_code = 9998 %}
with validations as (
    select k_lea,
        {{ edu_edfi_source.validation_struct(
            severity="'potential'",
            code=error_code,
            message="concat('This is a fake error for a lea.')"
        ) }} as validation
    from {{ ref('dim_lea') }}
    where k_lea = 'cabac7ef367c3ee38f306bfd30ff21c2'
)
select *
from validations