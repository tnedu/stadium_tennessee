{{
  config(
    materialized = "table",
    schema = "stg_data_errors"
  )
}}

{% set error_code = 3404 %}

with brule as (
    select tdoe_error_code, cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end, tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }}
    where tdoe_error_code = {{ error_code }}
      and rule_model = '{{ this.identifier }}'
),
student_schedule as (
    select * from {{ ref('bld_ef3__student_schedule_minutes_by_report_period') }}
),
student_ssd as (
    select * from {{ ref('bld_ef3__student_ssd_minutes_by_report_period') }}
),
under_scheduled as (
    select sch.k_student, sch.k_school, sch.k_school_calendar, sch.school_id,
        sch.student_unique_id, sch.school_year, sch.entry_date, sch.report_period,
        sch.sum_class_duration, ssd.sum_ssd_mins
    from student_schedule sch
    join student_ssd ssd
        on ssd.k_student = sch.k_student
       and ssd.k_school = sch.k_school
       and ssd.k_school_calendar = sch.k_school_calendar
       and ssd.school_id = sch.school_id
       and ssd.school_year = sch.school_year
       and ssd.entry_date = sch.entry_date
       and ssd.report_period = sch.report_period
    where sch.sum_class_duration < ssd.sum_ssd_mins
),
bad_records as (
    select us.k_student, us.school_id, us.student_unique_id, us.school_year, us.entry_date,
        string_agg(
            concat('Reporting Period: ', cast(us.report_period as string),
                ', Sum of class duration: ', coalesce(cast(us.sum_class_duration as string), '0'),
                ', Sum of SSD mins: ', coalesce(cast(us.sum_ssd_mins as string), '0')),
            '; '
        ) as report_period_info
    from under_scheduled us
    group by us.k_student, us.school_id, us.student_unique_id, us.school_year, us.entry_date
),
errors as (
    select bad.k_student, cast(null as string) as k_course_section, bad.school_id, bad.student_unique_id,
        cast(null as string) as local_course_code, bad.school_year, cast(null as string) as section_id,
        cast(null as date) as begin_date, s.state_student_id as legacy_state_student_id,
        brule.tdoe_error_code as error_code,
        concat('Student is under scheduled - Student Unique ID: ', coalesce(bad.student_unique_id, '[null]'),
            ', School ID: ', coalesce(cast(bad.school_id as string), '[null]'),
            ', Enrollment Date: ', coalesce(cast(bad.entry_date as string), '[null]'),
            ', ', bad.report_period_info, '.') as error,
        {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }}, brule.tdoe_severity
    from bad_records bad
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = bad.k_student
    join brule
        on bad.school_year between brule.error_school_year_start and brule.error_school_year_end
)
select * from errors