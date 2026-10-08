{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select *
from {{ ref('3400_begin_date') }}
union
select *
from {{ ref('3401_end_date') }}
union
select *
from {{ ref('3402_section_not_within_enrollment') }}
union
select *
from {{ ref('3403_over_scheduled') }}
union
select *
from {{ ref('3404_under_scheduled') }}
union
select *
from {{ ref('3405_section_dates_not_within_session_dates') }}
