{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_staff, tdoe_severity_code, tdoe_severity
    from {{ ref('staff_education_organization_assignment_associations') }}
)
select k_staff,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_staff