{{
  config(
    materialized="table",
    schema="cds"
  )
}}
select 
  courses.tenant_code, 
  courses.api_year,
  courses.k_course,
  max(case when course_level_characteristic = 'FUND' then true else false end) as is_cte,
  max(case when course_level_characteristic in ('Advanced Placement', 'CIE', 'DE', 'IGCSE', 'International Baccalaureate', 'SDC') then course_level_characteristic else null end) as EPSOIdentification,
  max(case when course_level_characteristic in ('CTE-ARTS', 'CTE-CA', 'CTE-EDU', 'CTE-FIN', 'CTE-CTE', 'CTE-HOSP', 'CTE-HUSV', 'CTE-IT', 'CTE-LAW', 'CTE-STEM', 'CTE-TRAN', 'CTE-WBL', 'CTE-GOV', 'CTE-HS', 'CTE-MKTG', 'CTE-MANU') then course_level_characteristic else null end) as CTE_Cluster,
  max(case when course_level_characteristic = 'General' then true else false end) as is_General,
from {{ ref('stg_ef3__courses__level_characteristics') }} courses
where courses.course_level_characteristic is not null
group by all