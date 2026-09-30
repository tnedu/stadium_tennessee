{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select *
from {{ ref('3700_secondary_options_wo_primary_options') }}
union
select *
from {{ ref('3701_unexpected_sped_disability_combinations') }}
union
select *
from {{ ref('3702_overlapping_sped_options') }}
union
select *
from {{ ref('3703_disallowed_sped_option_combinations') }}
union
select *
from {{ ref('3704_sped_associations_no_valid_enrollment') }}