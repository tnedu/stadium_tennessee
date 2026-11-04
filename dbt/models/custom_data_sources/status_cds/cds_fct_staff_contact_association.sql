{{
  config(
    materialized="table",
    schema="cds"
  )
}}

select k_staff, ed_org_id, contact_title, email_address,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from {{ ref('staff_education_organization_contact_associations') }}
group by k_staff, ed_org_id, contact_title, email_address