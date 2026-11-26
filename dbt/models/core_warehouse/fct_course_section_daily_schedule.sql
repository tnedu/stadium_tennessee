{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_course_section set not null"
    ]
  )
}}

with course_section_schedules as (
    select
        k_school,
        k_course_section,
        k_course,

        tenant_code,
        school_year,

        section_id,
        local_course_code,
        course_code,
        session_name,
        is_schedule_exempt,
        {{ schedule_object_select_fields('course_section', 'course_section_schedule' ) }}

        daily_schedule,

        has_overlapping_meeting_times,
        has_class_period_duplicate_dates,
        has_class_period_overlap

    from {{ ref('fct_course_section_schedule') }} course_section_schedule
    where size(daily_schedule) > 0
),
course_section_student_date_ranges as (
    select *
    from {{ ref('bld_ef3__course_section_student_date_ranges') }}
),
calendar_dates as (
    select
        k_school,
        calendar_date,
        is_school_day
    from {{ ref('dim_calendar_date') }}
),
daily_course_sections as (
    /* Grain: One course section x calendar date. */
    select
        schedules.k_school,
        schedules.k_course_section,
        schedules.k_course,

        schedules.tenant_code,
        schedules.school_year,

        schedules.section_id,
        schedules.local_course_code,
        schedules.course_code,
        schedules.session_name,
        schedules.is_schedule_exempt,
        {{ schedule_object_select_fields('course_section', 'schedules' ) }}

        {{ array_explode_value('schedule_day') }}.calendar_date::date
            as calendar_date,
        
        calendar_dates.is_school_day,

        case
            when calendar_dates.calendar_date is null
            then true
            else false
        end as is_missing_calendar_date,

        {{ array_explode_value('schedule_day') }}.first_start_time as first_start_time,
        {{ array_explode_value('schedule_day') }}.class_periods as class_periods,
        {{ array_explode_value('schedule_day') }}.scheduled_minutes as scheduled_minutes,
        {{ array_explode_value('schedule_day') }}.scheduled_minutes_without_overlap as scheduled_minutes_without_overlap,

        coalesce(
            schedules.has_overlapping_meeting_times,
            false
        ) as has_overlapping_meeting_times,
        coalesce(
            schedules.has_class_period_duplicate_dates,
            false
        ) as has_class_period_duplicate_dates,
        coalesce(
            schedules.has_class_period_overlap,
            false
        ) as has_class_period_overlap,
        (
            coalesce(
                schedules.has_overlapping_meeting_times,
                false
            )
            or coalesce(
                schedules.has_class_period_duplicate_dates,
                false
            )
            or coalesce(
                schedules.has_class_period_overlap,
                false
            )
        ) as has_section_schedule_issue
    from course_section_schedules schedules
        {{ array_explode(
            'schedules.daily_schedule',
            'schedule_day'
        ) }}
    left join calendar_dates
        on calendar_dates.k_school =
            schedules.k_school
        and calendar_dates.calendar_date =
            {{ array_explode_value('schedule_day') }}.calendar_date::date
),
daily_course_sections_with_students as (
    select
        daily_course_sections.*,
        case
            when student_ranges.k_course_section is not null
            then true
            else false
        end as has_students
    from daily_course_sections
    left join course_section_student_date_ranges student_ranges
        on student_ranges.k_course_section = daily_course_sections.k_course_section
        and student_ranges.tenant_code = daily_course_sections.tenant_code
        and student_ranges.school_year = daily_course_sections.school_year
        and daily_course_sections.calendar_date >= student_ranges.student_coverage_begin_date
        and (
            student_ranges.student_coverage_end_date is null
            or daily_course_sections.calendar_date <=
                student_ranges.student_coverage_end_date
        )
),
formatted as (
    select
        k_school,
        k_course_section,
        k_course,

        tenant_code,
        school_year,

        calendar_date,
        is_school_day,
        is_missing_calendar_date,

        section_id,
        local_course_code,
        course_code,
        session_name,
        is_schedule_exempt,
        {{ schedule_object_select_fields('course_section', 'daily_course_sections_with_students' ) }}

        has_students,

        first_start_time,

        class_periods,

        scheduled_minutes,
        scheduled_minutes_without_overlap,

        has_overlapping_meeting_times,
        has_class_period_duplicate_dates,
        has_class_period_overlap,
        has_section_schedule_issue
    from daily_course_sections_with_students
)
select *
from formatted
order by
    tenant_code,
    k_school,
    calendar_date,
    first_start_time,
    course_code,
    section_id