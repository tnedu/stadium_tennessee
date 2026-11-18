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
  sections.k_course_section,
  max(case when course_level_characteristic = 'FUND' then true else false end) as is_cte,
  max(case when course_level_characteristic in ('Advanced Placement', 'CIE', 'DE', 'IGCSE', 'International Baccalaureate', 'SDC') then course_level_characteristic else null end) as EPSOIdentification,
  max(case when course_level_characteristic like ('CTE-%') then course_level_characteristic else null end) as CTE_Cluster,
  max(case when course_level_characteristic = 'General' then true else false end) as is_General
from {{ ref('stg_ef3__courses__level_characteristics') }} courses
join {{ ref('stg_ef3__course_offerings') }} offerings
    on offerings.k_course = courses.k_course
join {{ ref('stg_ef3__sections') }} sections
    on sections.k_course_offering = offerings.k_course_offering
where courses.course_level_characteristic is not null
group by all