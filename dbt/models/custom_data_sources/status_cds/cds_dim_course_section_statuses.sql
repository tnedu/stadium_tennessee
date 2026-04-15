{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_course_section, tdoe_severity_code, tdoe_severity
    from {{ ref('sections') }}
)
select k_course_section,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_course_section