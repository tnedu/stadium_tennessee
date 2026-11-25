{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_student set not null",
    ]
  )
}}

with student_school_associations as (
    select
        k_student_school_association,
        k_student,
        k_school,
        tenant_code,
        school_year,
        entry_date,
        exit_withdraw_date
    from {{ ref('fct_student_school_association') }}
),
student_section_associations as (
    select
        k_student,
        k_school,
        k_course_section,
        tenant_code,
        school_year,
        begin_date,
        end_date
    from {{ ref('fct_student_section_association') }}
),
course_section_daily_schedules as (
    select *
    from {{ ref('fct_course_section_daily_schedule') }}
    where has_students
),
student_course_sections as (
    /* Grain: One student-school enrollment x StudentSectionAssociation. */
    select
        enrollments.k_student_school_association,
        enrollments.k_student,
        enrollments.k_school,
        enrollments.tenant_code,
        enrollments.school_year,
        sections.k_course_section,
        greatest(
            enrollments.entry_date,
            sections.begin_date
        ) as effective_begin_date,
        case
            when enrollments.exit_withdraw_date is null
                and sections.end_date is null
                then null
            when enrollments.exit_withdraw_date is null
                then sections.end_date
            when sections.end_date is null
                then enrollments.exit_withdraw_date
            else least(
                enrollments.exit_withdraw_date,
                sections.end_date
            )
        end as effective_end_date
    from student_school_associations enrollments
    join student_section_associations sections
        on sections.k_student = enrollments.k_student
        and sections.k_school = enrollments.k_school
        and sections.tenant_code = enrollments.tenant_code
        and sections.school_year = enrollments.school_year
        and sections.begin_date <= coalesce(
            enrollments.exit_withdraw_date,
            date('9999-12-31')
        )
        and coalesce(
            sections.end_date,
            date('9999-12-31')
        ) >= enrollments.entry_date
),
student_daily_course_sections as (
    select
        students.tenant_code,
        students.school_year,
        students.k_school,
        students.k_student_school_association,
        students.k_student,
        schedule.calendar_date,
        schedule.k_course_section,
        schedule.k_course,
        schedule.section_id,
        schedule.local_course_code,
        schedule.course_code,
        schedule.session_name,
        schedule.is_school_day,
        schedule.is_missing_calendar_date,
        schedule.first_start_time,
        schedule.class_periods,
        schedule.scheduled_minutes,
        schedule.scheduled_minutes_without_overlap,
        schedule.has_section_schedule_issue
    from student_course_sections students
    join course_section_daily_schedules schedule
        on schedule.k_course_section = students.k_course_section
        and schedule.k_school = students.k_school
        and schedule.tenant_code = students.tenant_code
        and schedule.school_year = students.school_year
        and schedule.calendar_date >= students.effective_begin_date
        and (
            students.effective_end_date is null
            or schedule.calendar_date <= students.effective_end_date
        )
),
student_daily_course_section_values as (
    /*
     * Build the section value only after all joins have reduced the data
     * to the exact student/enrollment/date/section grain.
     */
    select
        tenant_code,
        school_year,
        k_school,
        k_student_school_association,
        k_student,
        calendar_date,
        is_school_day,
        first_start_time,
        course_code,
        section_id,
        scheduled_minutes,
        scheduled_minutes_without_overlap,
        is_missing_calendar_date,
        has_section_schedule_issue,
        {{ edu_edfi_source.json_object_construct([
            ['k_course_section', 'k_course_section'],
            ['k_course', 'k_course'],
            ['section_id', 'section_id'],
            ['local_course_code', 'local_course_code'],
            ['course_code', 'course_code'],
            ['session_name', 'session_name'],
            ['first_start_time', 'first_start_time'],
            ['class_periods', 'class_periods'],
            ['scheduled_minutes', 'scheduled_minutes'],
            ['scheduled_minutes_without_overlap', 'scheduled_minutes_without_overlap'],
            ['has_section_schedule_issue', 'has_section_schedule_issue']
        ]) }} as course_section
    from student_daily_course_sections
),
student_daily_schedule_rollup as (
    /* Grain: One student-school enrollment x calendar date. */
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
        count(*) as course_section_count,
        sum(scheduled_minutes) as total_scheduled_minutes,
        /*
         * Overlap has been removed within each section, but not between
         * different sections assigned to the student.
         */
        sum(scheduled_minutes_without_overlap) as total_scheduled_minutes_without_section_overlap,
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
student_daily_schedule_issues as (
    select
        *,
        flatten(
            transform(
                /* Does there exist Section A */
                course_sections,
                section_a ->
                    transform(
                        filter(
                            /* where there exists Section B > A */
                            course_sections,
                            section_b ->
                                section_a.k_course_section < section_b.k_course_section
                                and exists(
                                    section_a.class_periods,
                                    period_a ->
                                        exists(
                                            /* where there exists a meeting time in Section A */
                                            period_a.meeting_times,
                                            time_a ->
                                                exists(
                                                    section_b.class_periods,
                                                    period_b ->
                                                        exists(
                                                            /* that overlaps with a meeting time in Section B? */
                                                            period_b.meeting_times,
                                                            time_b ->
                                                                time_a.start_time < time_b.end_time
                                                                and time_b.start_time < time_a.end_time
                                                        )
                                                )
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

    from student_daily_schedule_rollup
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
select * from formatted