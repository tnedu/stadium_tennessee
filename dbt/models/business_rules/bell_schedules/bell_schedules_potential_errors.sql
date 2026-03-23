{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('bell_schedules_errors_unioned') }}
where tdoe_severity = 'potential'