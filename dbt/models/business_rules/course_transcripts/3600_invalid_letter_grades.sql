{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}
{% set error_code = 3600 %}

/* Final Letter Grades must be from a set of accepted values.*/
{% set accepted_grades = ['A-', 'A', 'A+', 'B-', 'B', 'B+', 'C-', 'C', 'C+', 'D-', 'D', 'D+', 'F', 'I', 'P'] %}

with brule as (
    select tdoe_error_code, cast(error_school_year_start as int) as error_school_year_start, cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end, tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
stg_course_transcripts as (
    select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, ct.course_ed_org_id, ct.student_academic_record_ed_org_id,
        ct.student_unique_id, ct.academic_term, ct.alternative_course_code, ct.final_letter_grade_earned, brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__course_transcripts') }} ct
    join brule
        on cast(ct.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where ct.final_letter_grade_earned is not null
),
errors as (
    select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, ct.course_ed_org_id, ct.student_academic_record_ed_org_id,
        ct.student_unique_id, ct.academic_term, ct.alternative_course_code, ct.tdoe_error_code as error_code,
        concat('Student ', ct.student_unique_id, ' has a Final Letter Grade of ', ct.final_letter_grade_earned, ' for course ', ct.course_code, ', ',
            'which is not an accepted value ({{ accepted_grades | join(", ") }}, or null).') as error,
        {{ severity_to_severity_code_case_clause('ct.tdoe_severity') }}, ct.tdoe_severity
    from stg_course_transcripts ct
    where ct.final_letter_grade_earned not in ('{{ accepted_grades | join("', '") }}')
)
select *
from errors
