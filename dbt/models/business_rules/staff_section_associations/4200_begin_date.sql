{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4200 %}

with stg_staff_section_associations as (
    select * from {{ ref('stg_ef3__staff_section_associations') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
),
errors as (
    /* Staff Section Begin Date must be within the school year begin and end date. */
    select ssa.k_staff, ssa.k_course_section, ssa.local_course_code, ssa.school_year, ssa.school_id, 
        ssa.section_id, ssa.session_name, ssa.staff_unique_id, ssa.begin_date,
        {{ error_code }} as error_code,
        concat('Staff Section Association Begin Date does not fall within the school year. Value Received: ', ssa.begin_date, 
            '. The state school year starts ',
            concat((ssa.school_year-1), '-07-01'), ' and ends ', concat(ssa.school_year, '-06-30'), '.') as error
    from stg_staff_section_associations ssa
    where 
        not(ssa.begin_date between to_date(concat((ssa.school_year-1), '-07-01'), 'yyyy-MM-dd') 
            and to_date(concat(ssa.school_year, '-06-30'), 'yyyy-MM-dd'))
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}