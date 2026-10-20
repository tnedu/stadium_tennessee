{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('staff_education_organization_contact_associations_unioned') }}
where tdoe_severity != 'potential'