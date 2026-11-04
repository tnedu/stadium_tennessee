{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}
{% set error_code = 3601 %}

/* Final Numeric Grades must be between {{ min_grade }} and {{ max_grade }} (inclusive) or null. */
{% set min_grade = 0 %}
{% set max_grade = 105 %}

with brule as (
    select tdoe_error_code, cast(error_school_year_start as int) as error_school_year_start, cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end, tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
stg_course_transcripts as (
    select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, ct.course_ed_org_id, ct.student_academic_record_ed_org_id,
        ct.student_unique_id, ct.academic_term, ct.alternative_course_code, ct.final_numeric_grade_earned, brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__course_transcripts') }} ct
    join brule
        on cast(ct.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where ct.final_numeric_grade_earned is not null
),
errors as (
    select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, ct.course_ed_org_id, ct.student_academic_record_ed_org_id,
        ct.student_unique_id, ct.academic_term, ct.alternative_course_code, ct.tdoe_error_code as error_code,
        concat('Student ', ct.student_unique_id, ' has a Final Numeric Grade of ', ct.final_numeric_grade_earned, ' for course ', ct.course_code, ', ',
            'which is not between {{ min_grade }} and {{ max_grade }} (inclusive) or null.') as error,
        {{ severity_to_severity_code_case_clause('ct.tdoe_severity') }}, ct.tdoe_severity
    from stg_course_transcripts ct
    where ct.final_numeric_grade_earned < {{ min_grade }}
        or ct.final_numeric_grade_earned > {{ max_grade }}
)
select *
from errors
