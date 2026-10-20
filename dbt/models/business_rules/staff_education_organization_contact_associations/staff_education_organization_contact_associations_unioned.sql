{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select *
from {{ ref('4300_invalid_staff_email_domain_name') }}
union
select *
from {{ ref('4301_invalid_email_address_syntax') }}