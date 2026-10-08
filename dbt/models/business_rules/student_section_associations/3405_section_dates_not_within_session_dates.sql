{{
  config(
    materialized = "table",
    schema = "stg_data_errors"
  )
}}

{% set error_code = 3405 %}

with brule as (
    select tdoe_error_code, cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end, tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }}
    where tdoe_error_code = {{ error_code }}
      and rule_model = '{{ this.identifier }}'
),
bad_records as (
    select ssa.k_student, ssa.k_course_section, cast(ssa.school_id as int) as school_id,
        cast(ssa.school_year as int) as school_year, ssa.student_unique_id, ssa.local_course_code,
        ssa.section_id, ssa.session_name, ssa.begin_date as student_section_begin_date,
        ssa.end_date as student_section_end_date, sess.session_begin_date, sess.session_end_date
    from {{ ref('stg_ef3__student_section_associations') }} ssa
    join {{ ref('stg_ef3__sessions') }} sess
        on sess.school_id = ssa.school_id
       and sess.school_year = ssa.school_year
       and sess.session_name = ssa.session_name
    where ssa.begin_date > sess.session_end_date
       or (ssa.end_date is not null and ssa.end_date < sess.session_begin_date)
),
errors as (
    select bad.k_student, bad.k_course_section, bad.school_id, bad.student_unique_id,
        bad.local_course_code, bad.school_year, bad.section_id, bad.student_section_begin_date as begin_date,
        s.state_student_id as legacy_state_student_id, brule.tdoe_error_code as error_code,
        concat('Student Section dates are not within SESSION Dates - State Unique ID: ',
            coalesce(s.state_student_id, '[no value]'),
            ', StudentSectionAssociation BeginDate: ', cast(bad.student_section_begin_date as string),
            ', StudentSectionAssociation EndDate: ', coalesce(cast(bad.student_section_end_date as string), '[null]'),
            ', LocalCourseCode: ', coalesce(bad.local_course_code, '[null]'),
            ', SectionIdentifier: ', coalesce(bad.section_id, '[null]'),
            ', Session Name: ', coalesce(bad.session_name, '[null]'),
            ', Session Begin Date: ', cast(bad.session_begin_date as string),
            ', Session End Date: ', cast(bad.session_end_date as string), '.') as error,
        {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }}, brule.tdoe_severity
    from bad_records bad
    left join {{ ref('stg_ef3__students') }} s
        on s.k_student = bad.k_student
    join brule
        on bad.school_year between brule.error_school_year_start and brule.error_school_year_end
)
select * from errors