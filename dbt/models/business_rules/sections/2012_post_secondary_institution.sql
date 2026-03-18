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

-- sections that are LDC / DE
sections_with_ldc_de as (
    select
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
        split(course_level_struct.courseLevelCharacteristicDescriptor, '#')[1] as course_level_characteristic
    from stg_sections s
    lateral view explode(
        cast(
            s.v_course_level_characteristics
            as array<struct<courseLevelCharacteristicDescriptor:string>>
        )
    ) cl AS course_level_struct
    where split(course_level_struct.courseLevelCharacteristicDescriptor, '#')[1] in ('LDC', 'DE')
),

-- sections that HAVE a postsecondary institution
sections_with_postsecondary as (
    select distinct
        s.k_course_section
    from stg_sections s
    lateral view outer explode(
        cast(
            s.v_programs
            as array<struct<
                programReference:struct<
                    educationOrganizationId:int,
                    programName:string,
                    programTypeDescriptor:string,
                    link:struct<href:string, rel:string>
                >
            >>
        )
    ) p AS program_struct
    where program_struct.programReference.educationOrganizationId is not null
),

-- LDC / DE sections that are MISSING postsecondary institution
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
    from sections_with_ldc_de sw
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
