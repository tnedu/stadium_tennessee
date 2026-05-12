{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('student_school_associations_errors_unioned') }}
where tdoe_severity != 'potential'