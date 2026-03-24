{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2012 %}

with brule as (
    select
        tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),

-- sections in rule year range
stg_sections as (
    select *
    from {{ ref('stg_ef3__sections') }} s
    join brule
      on cast(s.school_year as int)
         between brule.error_school_year_start and brule.error_school_year_end
),

-- LDC sections (section-level characteristic)
ldc_sections as (
    select distinct
        s.k_course_section,
        s.k_course_offering,
        s.k_school,
        s.k_location,
        s.k_school__location,
        s.section_id,
        s.local_course_code,
        s.school_id,
        s.school_year,
        s.session_name,
        'LDC' as course_level_characteristic
    from stg_sections s
    join {{ ref('stg_ef3__sections__course_level_characteristics') }} cl
      on cl.k_course_section = s.k_course_section
    where cl.course_level_characteristic = 'LDC'
),

-- DE sections (course-level characteristic)
de_sections as (
    select distinct
        s.k_course_section,
        s.k_course_offering,
        s.k_school,
        s.k_location,
        s.k_school__location,
        s.section_id,
        s.local_course_code,
        s.school_id,
        s.school_year,
        s.session_name,
        'DE' as course_level_characteristic
    from stg_sections s
    join {{ ref('stg_ef3__course_offerings') }} co
      on co.k_course_offering = s.k_course_offering
    join {{ ref('stg_ef3__courses__level_characteristics') }} cl
      on cl.k_course = co.k_course
    where cl.course_level_characteristic = 'DE'
),

-- union LDC + DE sections
ldc_de_sections as (
    select * from ldc_sections
    union all
    select * from de_sections
),

-- sections that HAVE a valid postsecondary institution
sections_with_postsecondary as (
    select distinct
        sp.k_course_section
    from {{ ref('stg_ef3__sections__programs') }} sp
    join {{ ref('stg_ef3__programs') }} p
      on p.k_program = sp.k_program
    join {{ ref('stg_ef3__post_secondary_institutions') }} psi
      on psi.post_secondary_institution_id = p.ed_org_id
),

-- LDC / DE sections that are MISSING a valid postsecondary institution
errors as (
    select
        sw.k_course_section,
        sw.k_course_offering,
        sw.k_school,
        sw.k_location,
        sw.k_school__location,
        sw.section_id,
        sw.local_course_code,
        sw.school_id,
        sw.school_year,
        sw.session_name,
        {{ error_code }} as error_code,
        concat(
            'Post Secondary Institution must be submitted for LDC / DE sections. ',
            sw.school_id, ', ',
            sw.local_course_code, ', ',
            sw.section_id, ', ',
            sw.session_name, ', ',
            sw.course_level_characteristic, '.'
        ) as error
    from ldc_de_sections sw
    left join sections_with_postsecondary sp
      on sw.k_course_section = sp.k_course_section
    where sp.k_course_section is null
)

select
    errors.*,
    {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
    brule.tdoe_severity
from errors
join brule
  on errors.school_year between brule.error_school_year_start and brule.error_school_year_end;
