{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4200 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
stg_staff_section_associations as (
    select ssa.*,
            brule.tdoe_error_code,
            brule.tdoe_severity
    from {{ ref('stg_ef3__staff_section_associations') }} ssa
    join brule
        on cast(ssa.school_year as int)
           between brule.error_school_year_start and brule.error_school_year_end
),
errors as (
    /* Staff Section Begin Date must be within the school year begin and end date. */
    select ssa.k_staff, ssa.k_course_section, ssa.local_course_code, ssa.school_year, ssa.school_id, 
        ssa.section_id, ssa.session_name, ssa.staff_unique_id, ssa.begin_date,
        ssa.tdoe_error_code as error_code,
        concat('Staff Section Association Begin Date does not fall within the school year. Value Received: ', ssa.begin_date, 
            '. The state school year starts ',
            concat((ssa.school_year-1), '-07-01'), ' and ends ', concat(ssa.school_year, '-06-30'),
            ', Local Course Code: ', coalesce(ssa.local_course_code, '[no value]'),
            ', Section ID: ', coalesce(ssa.section_id, '[no value]'), '.') as error,
        {{ severity_to_severity_code_case_clause('ssa.tdoe_severity') }},
        ssa.tdoe_severity
    from stg_staff_section_associations ssa
    where 
        not(ssa.begin_date between to_date(concat((ssa.school_year-1), '-07-01'), 'yyyy-MM-dd') 
            and to_date(concat(ssa.school_year, '-06-30'), 'yyyy-MM-dd'))
)
select *
from errors 