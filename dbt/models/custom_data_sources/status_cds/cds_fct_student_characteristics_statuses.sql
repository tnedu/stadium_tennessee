{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (   
  select 
          seoa.k_student, 
          seoa.k_lea, 
          sc.student_characteristic, 
          sc.begin_date,
          seoa.tdoe_severity_code, 
          seoa.tdoe_severity
      from {{ ref('student_education_organization_associations') }} seoa
      left join {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
          on sc.k_student = seoa.k_student
          and sc.k_lea = seoa.k_lea
      where seoa.k_lea is not null
)
select k_student, k_lea, student_characteristic, begin_date,
    max(tdoe_severity_code) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)')}}
from all
group by k_student, k_lea, student_characteristic, begin_date