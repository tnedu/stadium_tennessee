{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (

    select 
        k_student, 
        k_lea, 
        student_characteristic, 
        begin_date, 
        tdoe_severity_code, 
        tdoe_severity
    from {{ ref('3006_student_characteristics_end_date') }}
    where tdoe_severity != 'potential'

    union all

    select 
        k_student, 
        k_lea, 
        student_characteristic, 
        begin_date, 
        tdoe_severity_code, 
        tdoe_severity
    from {{ ref('3007_student_characteristics_overlaps') }}
    where tdoe_severity != 'potential'
)

select 
    k_student, 
    k_lea, 
    student_characteristic, 
    begin_date,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)') }}
from all
group by 
    k_student, 
    k_lea, 
    student_characteristic, 
    begin_date