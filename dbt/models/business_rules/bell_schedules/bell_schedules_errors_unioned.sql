{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select *
from {{ ref('2008_dates_required') }}