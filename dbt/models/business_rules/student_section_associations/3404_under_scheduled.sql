{{
  config(
    materialized = "table",
    schema = "stg_data_errors"
  )
}}
{% set error_code = 3404 %}
with brule as (
    select tdoe_error_code, cast(error_school_year_start as int) as error_school_year_start, cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end, tdoe_severity
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
    select ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id, ssd.student_unique_id, ssd.school_year, ssd.entry_date, ssd.report_period,
        coalesce(sch.sum_class_duration, 0) as sum_class_duration, ssd.sum_ssd_mins
    from student_ssd ssd
    left join student_schedule sch
        on sch.k_student = ssd.k_student
       and sch.k_school = ssd.k_school
       and sch.k_school_calendar = ssd.k_school_calendar
       and sch.entry_date = ssd.entry_date
       and sch.report_period = ssd.report_period
    where coalesce(sch.sum_class_duration, 0) < ssd.sum_ssd_mins
),
bad_records as (
    select us.k_student, us.k_school, us.school_id, us.student_unique_id, us.school_year, us.entry_date,
        concat_ws('; ', transform(array_sort(collect_list(named_struct('rp', us.report_period, 'msg',
            concat('Reporting Period: ', cast(us.report_period as string), ', Sum of class duration: ', cast(us.sum_class_duration as string), ', Sum of SSD mins: ', cast(us.sum_ssd_mins as string))))), x -> x.msg)) as report_period_info
    from under_scheduled us
    group by us.k_student, us.k_school, us.school_id, us.student_unique_id, us.school_year, us.entry_date
),
errors as (
    select bad.k_student, cast(null as string) as k_course_section, bad.school_id, bad.student_unique_id, cast(null as string) as local_course_code, bad.school_year,
        cast(null as string) as section_id, bad.entry_date as begin_date, s.state_student_id as legacy_state_student_id, brule.tdoe_error_code as error_code,
        concat('Student is under scheduled - Student Unique ID: ', coalesce(bad.student_unique_id, '[null]'), ', School ID: ', coalesce(cast(bad.school_id as string), '[null]'),
            ', Enrollment Date: ', coalesce(cast(bad.entry_date as string), '[null]'), ', ', bad.report_period_info, '.') as error,
        {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }}, brule.tdoe_severity
    from bad_records bad
    left join {{ ref('stg_ef3__students') }} s
        on s.k_student = bad.k_student
    join brule
        on bad.school_year between brule.error_school_year_start and brule.error_school_year_end
)
select * from errors
