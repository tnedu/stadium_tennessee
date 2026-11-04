{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('course_transcripts_errors_unioned') }}
where tdoe_severity != 'potential'