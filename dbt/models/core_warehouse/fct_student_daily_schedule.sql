{{
  config(
    materialized="table",
    schema="wh"
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

course_section_daily_schedules as (

    select
        schedule.tenant_code,
        schedule.school_year,

        schedule.k_school,
        schedule.k_course_section,
        schedule.k_course,

        schedule.calendar_date,

        schedule.section_id,
        schedule.local_course_code,
        schedule.course_code,
        schedule.session_name,
        schedule.is_schedule_exempt,

        {{ schedule_object_select_fields(
            'course_section',
            'schedule'
        ) }}

        schedule.is_school_day,
        schedule.is_missing_calendar_date,

        schedule.first_start_time,
        schedule.class_periods,

        schedule.scheduled_minutes,
        schedule.scheduled_minutes_without_overlap,

        schedule.has_section_schedule_issue

    from {{ ref('fct_course_section_daily_schedule') }} schedule

    where schedule.has_students

),

student_daily_course_sections as (

    /*
     * Grain:
     * One student-school enrollment x StudentSectionAssociation
     * x calendar date x course section.
     *
     * Multiple StudentSectionAssociations for the same section remain
     * separate here. They are not silently deduplicated.
     */
    select
        periods.k_student_course_section_period,
        periods.k_student_school_association,

        periods.k_student,
        periods.k_school,

        periods.tenant_code,
        periods.school_year,

        periods.begin_date,

        schedule.calendar_date,

        schedule.k_course_section,
        schedule.k_course,

        schedule.section_id,
        schedule.local_course_code,
        schedule.course_code,
        schedule.session_name,
        schedule.is_schedule_exempt,

        {{ schedule_object_select_fields(
            'course_section',
            'schedule'
        ) }}

        schedule.is_school_day,
        schedule.is_missing_calendar_date,

        schedule.first_start_time,
        schedule.class_periods,

        schedule.scheduled_minutes,
        schedule.scheduled_minutes_without_overlap,

        schedule.has_section_schedule_issue

    from student_course_section_periods periods
    join course_section_daily_schedules schedule
        on schedule.k_course_section = periods.k_course_section
        and schedule.calendar_date >= periods.effective_begin_date
        and (
            periods.effective_end_date is null
            or schedule.calendar_date <= periods.effective_end_date
        )
),
student_daily_course_section_values as (

    /*
     * Build the full reporting object and the smaller overlap-check
     * object after the section/date applicability join.
     */
    select
        tenant_code,
        school_year,

        k_school,
        k_student_school_association,
        k_student,

        calendar_date,

        is_school_day,
        is_missing_calendar_date,

        first_start_time,
        course_code,
        section_id,

        scheduled_minutes,
        scheduled_minutes_without_overlap,

        has_section_schedule_issue,

        {% set section_fields = [
            ['k_course_section', 'k_course_section'],
            ['k_course', 'k_course'],
            ['section_id', 'section_id'],
            ['local_course_code', 'local_course_code'],
            ['course_code', 'course_code'],
            ['session_name', 'session_name'],
            ['is_schedule_exempt', 'is_schedule_exempt']
        ] %}

        {% set section_fields =
            section_fields
            + schedule_object_json_fields(
                'course_section',
                'student_daily_course_sections'
            )
        %}

        {% do section_fields.append(
            ['first_start_time', 'first_start_time']
        ) %}

        {% do section_fields.append(
            ['scheduled_minutes', 'scheduled_minutes']
        ) %}

        {% do section_fields.append(
            ['scheduled_minutes_without_overlap', 'scheduled_minutes_without_overlap']
        ) %}

        {% do section_fields.append(
            ['class_periods', 'class_periods']
        ) %}

        {% do section_fields.append(
            ['has_section_schedule_issue', 'has_section_schedule_issue']
        ) %}

        {{ edu_edfi_source.json_object_construct(
            section_fields
        ) }} as course_section,

        /*
         * Compact object used only for student-level overlap detection.
         *
         * Meeting times are flattened in place across class periods,
         * avoiding repeated traversal of class_periods[] during every
         * section-pair comparison.
         */
        named_struct(
            'k_course_section',
                k_course_section,

            'course_code',
                course_code,

            'section_id',
                section_id,

            'is_schedule_exempt',
                coalesce(
                    is_schedule_exempt,
                    false
                ),

            'meeting_times',
                flatten(
                    transform(
                        class_periods,
                        class_period ->
                            class_period.meeting_times
                    )
                )
        ) as course_section_overlap_check

    from student_daily_course_sections
),
student_daily_schedule_rollup as (
    /*
     * Grain:
     * One student-school enrollment x calendar date.
     */
    select
        k_student,
        k_school,
        k_student_school_association,

        tenant_code,
        school_year,

        calendar_date,

        max(is_school_day) as is_school_day,

        max(
            case
                when is_missing_calendar_date then 1
                else 0
            end
        ) = 1 as is_missing_calendar_date,

        {{ edu_edfi_source.json_array_agg(
            'course_section',
            order_by='first_start_time, course_code, section_id'
        ) }} as course_sections,

        {{ edu_edfi_source.json_array_agg(
            'course_section_overlap_check',
            order_by='first_start_time, course_code, section_id'
        ) }} as course_section_overlap_checks,

        count(*) as course_section_count,

        sum(
            scheduled_minutes
        ) as total_scheduled_minutes,

        /*
         * Overlap has been removed within each section, but not between
         * different sections assigned to the student.
         */
        sum(
            scheduled_minutes_without_overlap
        ) as total_scheduled_minutes_without_section_overlap,

        max(
            case
                when has_section_schedule_issue then 1
                else 0
            end
        ) = 1 as has_section_schedule_issue

    from student_daily_course_section_values

    group by
        tenant_code,
        school_year,
        k_school,
        k_student_school_association,
        k_student,
        calendar_date

),
student_daily_schedule_overlap_candidates as (
    /*
     * Remove schedule-exempt sections once, before performing any
     * pairwise meeting-time comparisons.
     */
    select
        *,
        filter(
            course_section_overlap_checks,
            section ->
                not coalesce(
                    section.is_schedule_exempt,
                    false
                )
        ) as overlap_candidate_sections
    from student_daily_schedule_rollup
),
student_daily_schedule_issues as (

    select
        * except (
            course_section_overlap_checks,
            overlap_candidate_sections
        ),

        /*
         * Compare each section pair once. An empty or single-element
         * candidate array naturally produces an empty result.
         *
         * For now, pair ordering uses k_course_section. We still need to
         * change this to StudentSectionAssociation identity so repeated
         * assignments to the same section can be compared.
         */
        flatten(
            transform(
                overlap_candidate_sections,
                section_a ->
                    transform(
                        filter(
                            overlap_candidate_sections,
                            section_b ->
                                section_a.k_course_section < section_b.k_course_section
                                and exists(
                                    section_a.meeting_times,
                                    time_a ->
                                        exists(
                                            section_b.meeting_times,
                                            time_b ->
                                                time_a.start_time < time_b.end_time
                                                and time_b.start_time < time_a.end_time
                                        )
                                )
                        ),
                        section_b ->
                            named_struct(
                                'k_course_section',
                                    section_a.k_course_section,

                                'course_code',
                                    section_a.course_code,

                                'section_id',
                                    section_a.section_id,

                                'overlaps_with',
                                    named_struct(
                                        'k_course_section',
                                            section_b.k_course_section,

                                        'course_code',
                                            section_b.course_code,

                                        'section_id',
                                            section_b.section_id
                                    )
                            )
                    )
            )
        ) as overlapping_course_sections

    from student_daily_schedule_overlap_candidates

),

formatted as (

    select
        k_student,
        k_school,
        k_student_school_association,

        tenant_code,
        school_year,

        calendar_date,

        is_school_day,
        is_missing_calendar_date,

        course_sections,
        course_section_count,

        total_scheduled_minutes,
        total_scheduled_minutes_without_section_overlap,

        has_section_schedule_issue,

        size(overlapping_course_sections) > 0 as has_course_section_overlap,

        overlapping_course_sections

    from student_daily_schedule_issues

)

select *
from formatted