{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4202 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
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
    where ssa.end_date is not null
), 
errors as (
    /* End Date must be greater than Begin Date. */
    select ssa.k_staff, ssa.k_course_section, ssa.local_course_code, ssa.school_year, ssa.school_id, 
        ssa.section_id, ssa.session_name, ssa.staff_unique_id, ssa.begin_date,
        ssa.tdoe_error_code as error_code,
        concat('Staff Section Association End Date must be greater than the Begin Date. End Date received: ',
            ssa.end_date, ', Begin Date: ', ssa.begin_date, 
            ', Local Course Code: ', coalesce(ssa.local_course_code, '[no value]'),
            ', Section ID: ', coalesce(ssa.section_id, '[no value]'), '.') as error,
        {{ severity_to_severity_code_case_clause('ssa.tdoe_severity') }},
        ssa.tdoe_severity
    from stg_staff_section_associations ssa
    where ssa.end_date < ssa.begin_date
)
select *
from errors