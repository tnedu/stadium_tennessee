{{
  config(
    materialized="table",
    schema="cds"
  )
}}
with course_chars as (
    select 
        courses.tenant_code, 
        courses.api_year,
        courses.k_course,
       {{ edu_edfi_source.extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as characteristic
    from {{ ref('stg_ef3__courses') }} courses
        {{ edu_edfi_source.json_flatten('courses.v_level_characteristics', outer=True) }}
)
select 
    tenant_code,
    api_year,
    k_course,
    characteristic as course_level_characteristic
from course_chars
where characteristic is not null
group by all