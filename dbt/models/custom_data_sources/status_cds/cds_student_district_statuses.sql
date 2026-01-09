{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_student, k_lea, ed_org_id, tdoe_severity_code, tdoe_severity
    from {{ ref('student_education_organization_associations') }}
    where k_lea is not null
)
select k_student, k_lea, ed_org_id,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_lea, ed_org_id