{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select *
from {{ ref('2008_dates_required') }}
union
select *
from {{ ref('2015_BSdate_not_an_ID_day') }}