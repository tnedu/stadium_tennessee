{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select k_class_period, tdoe_severity_code, tdoe_severity
    from {{ ref('class_periods') }}
)
select k_class_period,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_class_period