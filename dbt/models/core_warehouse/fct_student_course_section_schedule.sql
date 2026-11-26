{{
  config(
    materialized="table",
    schema="wh",
    liquid_clustered_by=["k_course_section"],
    post_hook=[ 
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_course_section set not null"
    ]
  )
}}

with student_course_section_periods as (

    select
        k_student_course_section_period,
        k_student_school_association,

        k_student,
        k_school,
        k_course_section,

        tenant_code,
        school_year,

        begin_date,
        effective_begin_date,
        effective_end_date

    from {{ ref('bld_ef3__student_course_section_period') }}

),
course_section_schedules as (
    select
        k_school,
        k_course_section,
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
),
student_schedules as (
    select
        schedule_periods.k_student_school_association,
        schedule_periods.k_student,
        schedule_periods.k_school,
        schedule_periods.k_course_section,
        schedule_periods.tenant_code,
        schedule_periods.school_year,
        schedule_periods.begin_date,
        section_schedules.section_id,
        section_schedules.local_course_code,
        section_schedules.course_code,
        section_schedules.session_name,
        section_schedules.is_schedule_exempt,
        {{ schedule_object_select_fields('course_section', 'section_schedules' ) }}
        schedule_periods.effective_begin_date,
        schedule_periods.effective_end_date,
        filter(
            section_schedules.daily_schedule,
            schedule_day ->
                schedule_day.calendar_date >=
                    schedule_periods.effective_begin_date
                and (
                    schedule_periods.effective_end_date is null
                    or schedule_day.calendar_date <=
                        schedule_periods.effective_end_date
                )
        ) as daily_schedule,

        /*
         * This indicates that the inherited course-section schedule
         * contains at least one known structural scheduling issue.
         *
         * It does not claim that every issue date falls within this
         * enrollment-specific schedule period.
         */
        (
            coalesce(
                section_schedules.has_overlapping_meeting_times,
                false
            )
            or coalesce(
                section_schedules.has_class_period_duplicate_dates,
                false
            )
            or coalesce(
                section_schedules.has_class_period_overlap,
                false
            )
        ) as has_section_schedule_issue

    from student_course_section_periods  schedule_periods
    join course_section_schedules section_schedules
        on section_schedules.k_course_section = schedule_periods.k_course_section
        and section_schedules.k_school = schedule_periods.k_school
        and section_schedules.tenant_code = schedule_periods.tenant_code
        and section_schedules.school_year = schedule_periods.school_year
    /*
     * This should already be guaranteed by the interval-overlap join,
     * but retaining the condition documents and protects the final grain.
     */
    where schedule_periods.effective_end_date is null
        or schedule_periods.effective_begin_date <= schedule_periods.effective_end_date
),
student_schedule_measures as (
    select
        *,
        size(daily_schedule) as date_count,

        case
            when size(daily_schedule) > 0
            then daily_schedule[0].calendar_date::date
        end as first_schedule_date,
        case
            when size(daily_schedule) > 0
            then daily_schedule[size(daily_schedule) - 1].calendar_date::date
        end as last_schedule_date,

        aggregate(
            daily_schedule,
            cast(0 as bigint),
            (total, schedule_day) ->
                total + schedule_day.scheduled_minutes
        ) as total_scheduled_minutes,
        aggregate(
            daily_schedule,
            cast(0 as bigint),
            (total, schedule_day) ->
                total + schedule_day.scheduled_minutes_without_overlap
        ) as total_scheduled_minutes_without_overlap
    from student_schedules
),
formatted as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'k_student_school_association',
            'k_course_section',
            'begin_date'
        ]) }} as k_student_course_section_schedule,
        k_student,
        k_school,
        k_student_school_association,
        k_course_section,
        tenant_code,
        school_year,
        /*
         * Part of the StudentSectionAssociation natural key.
         * It distinguishes repeated associations between the same
         * student and course section.
         */
        begin_date,
        section_id,
        local_course_code,
        course_code,
        session_name,
        is_schedule_exempt,
        {{ schedule_object_select_fields('course_section', 'student_schedule_measures' ) }}

        effective_begin_date,
        effective_end_date,
        daily_schedule,
        size(daily_schedule) as date_count,
        first_schedule_date, 
        last_schedule_date,
        total_scheduled_minutes,
        total_scheduled_minutes_without_overlap,
        has_section_schedule_issue
    from student_schedule_measures
)
select * from formatted