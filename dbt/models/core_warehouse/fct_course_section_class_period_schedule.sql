{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} alter column k_bell_schedule set not null"
    ]
  )
}}

with course_sections as (
    select * from {{ ref('dim_course_section') }}
),
class_periods as (
    select * from {{ ref('dim_class_period') }}
),
bell_schedules as (
    select * from {{ ref('dim_bell_schedule') }}
),
course_section_class_periods as (
    select * from {{ ref('brg_course_section_class_period') }}
),
bell_schedule_class_periods as (
    select * from {{ ref('brg_bell_schedule_class_period') }}
),
course_section_students as (
    select
        k_course_section,
        true as has_students
    from {{ ref('fct_student_section_association') }}
    group by k_course_section
),
schedule_rows as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'course_sections.k_course_section',
            'class_periods.k_class_period',
            'bell_schedules.k_bell_schedule'
        ]) }} as k_course_section_class_period_schedule,
        course_sections.k_school,
        course_sections.k_course_section,
        course_sections.k_course,
        class_periods.k_class_period,
        bell_schedules.k_bell_schedule,
        course_sections.tenant_code,
        course_sections.school_year,
        course_sections.section_id,
        course_sections.local_course_code,
        course_sections.course_code,
        course_sections.session_name,
        course_sections.is_schedule_exempt,
        {{ schedule_object_select_fields(object_name='course_section', source_alias='course_sections') }}
        coalesce(
            course_section_students.has_students,
            false
        ) as has_students,
        class_periods.class_period_name,
        class_periods.period_duration,
        class_periods.period_duration_without_overlap,
        class_periods.meeting_times,
        class_periods.meeting_time_count,
        {{ schedule_object_select_fields(object_name='class_period', source_alias='class_periods') }}
        bell_schedules.bell_schedule_name,
        bell_schedules.dates,
        bell_schedules.inferred_weekdays,
        bell_schedules.first_date,
        bell_schedules.last_date,
        bell_schedules.date_count,
        {{ schedule_object_select_fields(object_name='bell_schedule', source_alias='bell_schedules') }}
        ( class_periods.period_duration * bell_schedules.date_count ) as scheduled_minutes,
        ( class_periods.period_duration_without_overlap * bell_schedules.date_count ) as scheduled_minutes_without_overlap,
        class_periods.has_overlapping_meeting_times,
        bell_schedules.non_school_dates,
        bell_schedules.missing_calendar_dates,
        bell_schedule_class_periods.has_class_period_duplicate_dates,
        bell_schedule_class_periods.class_period_duplicate_schedules
    from course_sections
    join course_section_class_periods
        on course_section_class_periods.k_course_section = course_sections.k_course_section
    join class_periods
        on class_periods.k_class_period = course_section_class_periods.k_class_period
    join bell_schedule_class_periods
        on bell_schedule_class_periods.k_class_period = class_periods.k_class_period
    join bell_schedules
        on bell_schedules.k_bell_schedule = bell_schedule_class_periods.k_bell_schedule
    left join course_section_students
        on course_section_students.k_course_section = course_sections.k_course_section
),
schedule_dates as (
    /* Grain: One configured date per course-section/class-period/bell-schedule row. */
    select
        schedules.k_course_section_class_period_schedule,
        schedules.k_course_section,
        schedules.k_class_period,
        schedules.k_bell_schedule,
        {{ array_explode_value('schedule_date') }}::date as calendar_date
    from schedule_rows schedules
        {{ array_explode('schedules.dates', 'schedule_date') }}
),
schedule_intervals as (
    /*
     * Grain: One date and meeting-time interval per schedule row.
     *
     * Using the detail table avoids warehouse-specific access to fields
     * inside the meeting_times array of structs.
     */
    select
        schedule_dates.k_course_section_class_period_schedule,
        schedule_dates.k_course_section,
        schedule_dates.k_class_period,
        schedule_dates.k_bell_schedule,
        schedule_dates.calendar_date,
        meeting_times.meeting_time_sequence,
        meeting_times.start_time,
        meeting_times.end_time,
        meeting_times.military_start_time,
        meeting_times.military_end_time
    from schedule_dates
    join {{ ref('dtl_class_period_meeting_time') }} meeting_times
        on meeting_times.k_class_period = schedule_dates.k_class_period
),
schedule_overlap_pairs as (
    /*
     * Grain: One conflicting pair of schedule rows on one date.
     *
     * The schedule-row key comparison ensures:
     *   - a row is not compared with itself
     *   - each pair is evaluated only once
     *
     * Adjacent intervals are not overlapping because the interval
     * comparison uses strict inequality.
     */
    select distinct
        a.k_course_section,
        a.k_course_section_class_period_schedule as k_schedule_row_a,
        a.k_class_period as k_class_period_a,
        a.k_bell_schedule as k_bell_schedule_a,
        b.k_course_section_class_period_schedule as k_schedule_row_b,
        b.k_class_period as k_class_period_b,
        b.k_bell_schedule as k_bell_schedule_b,
        a.calendar_date
    from schedule_intervals a
    join schedule_intervals b
        on a.k_course_section = b.k_course_section
        and a.calendar_date = b.calendar_date
        /* This check is specifically for different class periods. */
        and a.k_class_period <> b.k_class_period
        and a.k_course_section_class_period_schedule < b.k_course_section_class_period_schedule
        and a.military_start_time < b.military_end_time
        and b.military_start_time < a.military_end_time
),
schedule_overlap_participants as (
    /*
     * Mark both rows participating in each conflicting pair.
     */
    select
        k_schedule_row_a as k_course_section_class_period_schedule,
        k_schedule_row_b as k_overlapping_schedule,
        calendar_date
    from schedule_overlap_pairs
    union
    select
        k_schedule_row_b as k_course_section_class_period_schedule,
        k_schedule_row_a as k_overlapping_schedule,
        calendar_date
    from schedule_overlap_pairs
),
schedule_overlap_details as (
    select
        overlaps.k_course_section_class_period_schedule,
        overlaps.k_overlapping_schedule,
        other.k_class_period,
        other.class_period_name,
        other.k_bell_schedule,
        other.bell_schedule_name,
        {{ edu_edfi_source.json_array_agg(
            'overlaps.calendar_date',
            order_by='overlaps.calendar_date'
        ) }} as dates
    from (
        select distinct
            k_course_section_class_period_schedule,
            k_overlapping_schedule,
            calendar_date
        from schedule_overlap_participants
    ) overlaps
    join schedule_rows other
        on other.k_course_section_class_period_schedule = overlaps.k_overlapping_schedule
    group by
        overlaps.k_course_section_class_period_schedule,
        overlaps.k_overlapping_schedule,
        other.k_class_period,
        other.class_period_name,
        other.k_bell_schedule,
        other.bell_schedule_name
),
schedule_overlap_rollup as (
    select
        k_course_section_class_period_schedule,
        true as has_class_period_overlap,
        {{ edu_edfi_source.json_array_agg(
            edu_edfi_source.json_object_construct([
                ['k_class_period', 'k_class_period'],
                ['class_period_name', 'class_period_name'],
                ['k_bell_schedule', 'k_bell_schedule'],
                ['bell_schedule_name', 'bell_schedule_name'],
                ['dates', 'dates']
            ])
        ) }} as overlapping_class_periods
    from schedule_overlap_details
    group by k_course_section_class_period_schedule
),
formatted as (
    select
        schedule_rows.*,
        coalesce(
            schedule_overlap_rollup.has_class_period_overlap,
            false
        ) as has_class_period_overlap,
        schedule_overlap_rollup.overlapping_class_periods
    from schedule_rows
    left join schedule_overlap_rollup
        on schedule_overlap_rollup.k_course_section_class_period_schedule = schedule_rows.k_course_section_class_period_schedule
)
select * from formatted