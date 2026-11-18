{{
  config(
    materialized="table",
    schema="build"
  )
}}
-- we are not a multi-tenant configuration, so our tenant owns all our leas
select distinct tenant_code, lea_id
from {{ ref('stg_ef3__local_education_agencies') }}