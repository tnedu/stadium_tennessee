{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('student_section_associations_unioned') }}
where tdoe_severity != 'potential'