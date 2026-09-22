{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select * 
from {{ ref('stg_ef3__schools')}}
where school_type = 'Public SPED Service Only School'
