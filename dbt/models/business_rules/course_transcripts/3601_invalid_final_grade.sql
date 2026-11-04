{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3601 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_course_transcripts as (
    select  ct.*,
            brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__course_transcripts') }} ct
    join brule
        on cast(ct.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    /* Ignore service schools for this rule. */
    where not exists (
            select 1
            from {{ ref('service_schools') }} ss
            where ct.student_academic_record_ed_org_id = ss.school_id
        )
),
errors as (
    /* Final Numeric Grades must be from a set of accepted values. */
    select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, 
        ct.course_ed_org_id, ct.student_academic_record_ed_org_id, ct.student_unique_id, ct.academic_term,
        ct.alternative_course_code,
        ct.tdoe_error_code as error_code,
        concat('Final Numeric Grade must be between 0 and 105 (inclusive) or [null]. Final Numeric Grade Received: ',
            ifnull(ct.final_numeric_grade_earned, '[null]'), '.') as error,
        {{ severity_to_severity_code_case_clause('ct.tdoe_severity') }},
        ct.tdoe_severity
    from stg_course_transcripts ct
    where ct.final_numeric_grade_earned is not null
        and (ct.final_numeric_grade_earned < 0
            or ct.final_numeric_grade_earned > 105)
)
select *
from errors