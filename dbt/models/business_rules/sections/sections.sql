{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('sections_errors_unioned') }}
where tdoe_severity != 'potential'