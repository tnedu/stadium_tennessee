{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('student_special_education_program_associations_errors_unioned') }}
where tdoe_severity != 'potential'