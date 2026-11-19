{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get a Student's Classes by Day for ADM.
*/

with student_classes as (
    /*
     * Produce one row per student/class-period occurrence.
     *
     * Grade and SPED FTE candidates have already been determined in
     * student_days. The CTE FTE candidate has already been determined in
     * course_section_days.
     *
     * No aggregation is needed here because the FTE lookup no longer
     * multiplies each class occurrence into multiple program candidates.
     *
     * Duplicate or overlapping student-section associations are intentionally
     * preserved. They represent the submitted data and may affect ADM.
     */
    select
        sm.k_student,
        sm.k_school,
        sm.k_school_calendar,
        sm.school_year,
        sm.is_primary_school,
        sm.entry_date,
        sm.calendar_date,

        fssa.k_course_section,
        fssa.begin_date as fssa_begin_date,
        fssa.end_date as fssa_end_date,

        si.course_code,
        si.start_time,
        si.end_time,
        coalesce(si.period_duration, 0) as period_duration,
        si.is_cte,
        si.cte_cluster,

        sm.fte_grade_weight,
        sm.fte_grade_statutory_program,
        sm.fte_sped_weight,
        sm.fte_sped_statutory_program,
        si.fte_cte_weight,
        si.fte_cte_statutory_program,

        /*
         * Select the current highest-weighted FTE candidate.
         *
         * Ties use this priority:
         *   1. CTE
         *   2. SPED
         *   3. Grade
         *
         * The individual candidate columns are also retained so this logic
         * can later change to SPED plus the greater of Grade and CTE.
         */
        case
            when si.fte_cte_weight is not null
                and si.fte_cte_weight >= greatest(
                    coalesce(sm.fte_grade_weight, -1),
                    coalesce(sm.fte_sped_weight, -1)
                )
                then si.fte_cte_statutory_program

            when sm.fte_sped_weight is not null
                and sm.fte_sped_weight >= coalesce(
                    sm.fte_grade_weight,
                    -1
                )
                then sm.fte_sped_statutory_program

            else sm.fte_grade_statutory_program
        end as fteada_program,

        case
            when sm.fte_grade_weight is null
                and sm.fte_sped_weight is null
                and si.fte_cte_weight is null
                then null

            else greatest(
                coalesce(sm.fte_grade_weight, -1),
                coalesce(sm.fte_sped_weight, -1),
                coalesce(si.fte_cte_weight, -1)
            )
        end as fteada_weight,

        greatest(
            coalesce(sm.tdoe_severity_code, 0),
            coalesce(fssa.tdoe_severity_code, 0),
            coalesce(si.tdoe_severity_code, 0)
        ) as tdoe_severity_code

    from {{ ref('fct_student_section_association') }} fssa

    join {{ ref('course_section_days') }} si
        on si.school_year = fssa.school_year
        and si.k_school = fssa.k_school
        and si.k_course_section = fssa.k_course_section
        and si.calendar_date >= fssa.begin_date
        and (
            fssa.end_date is null
            or si.calendar_date <= fssa.end_date
        )

    join {{ ref('student_days') }} sm
        on sm.school_year = fssa.school_year
        and sm.k_student = fssa.k_student
        and sm.k_school = fssa.k_school
        and sm.calendar_date = si.calendar_date
        and sm.is_early_grad_date = 0
),

class_meetings_with_overlap as (
    /*
     * Identify whether a meeting overlaps the next chronological meeting in
     * the student's complete daily schedule.
     */
    select
        *,
        case
            when end_time > lead(start_time) over (
                partition by
                    k_student,
                    k_school,
                    k_school_calendar,
                    is_primary_school,
                    entry_date,
                    calendar_date
                order by
                    start_time,
                    end_time,
                    course_code,
                    k_course_section
            ) then 1
            else 0
        end as has_overlapping_period

    from student_classes
),

student_courses_aggregated as (
    /*
     * Aggregate all scheduled meetings for the same course into one course
     * record for the student-day.
     *
     * scheduled_section_count is the number of distinct Course Sections.
     *
     * scheduled_meeting_time_count is the number of schedule occurrences.
     * Duplicate student-section associations remain included because they
     * represent the submitted data and affect the calculated duration.
     */
    select
        k_student,
        k_school,
        k_school_calendar,
        school_year,
        is_primary_school,
        entry_date,
        calendar_date,
        course_code,

        count(
            distinct struct(
                k_course_section,
                fssa_begin_date,
                fssa_end_date
            )
        ) as scheduled_section_count,

        count(*) as scheduled_meeting_time_count,

        sum(period_duration) as course_duration,

        max(is_cte) as is_cte,

        max(has_overlapping_period) as has_overlapping_periods,

        struct(
            min(start_time) as min_start_time,

            course_code,

            count(
                distinct struct(
                    k_course_section,
                    fssa_begin_date,
                    fssa_end_date
                )
            ) as scheduled_section_count,

            count(*) as scheduled_meeting_time_count,

            sum(period_duration) as course_duration,

            max(
                case
                    when is_cte then 1
                    else 0
                end
            ) as is_vocational_course,

            max(cte_cluster) as cte_cluster,

            max(fte_grade_weight) as fte_grade_weight,
            max(fte_grade_statutory_program) as fte_grade_statutory_program,

            max(fte_sped_weight) as fte_sped_weight,
            max(fte_sped_statutory_program) as fte_sped_statutory_program,

            max(fte_cte_weight) as fte_cte_weight,
            max(fte_cte_statutory_program) as fte_cte_statutory_program,

            max(fteada_weight) as fteada_weight,
            max_by(
                fteada_program,
                fteada_weight
            ) as fteada_program,

            /*
             * Duplicate meeting times are intentionally retained. They can
             * indicate duplicate or overlapping Student Section Associations
             * and explain an inflated course duration.
             */
            sort_array(
                collect_list(
                    concat(start_time, '-', end_time)
                )
            ) as meeting_times

        ) as course_info,

        max(tdoe_severity_code)
            as tdoe_severity_code

    from class_meetings_with_overlap

    group by
        k_student,
        k_school,
        k_school_calendar,
        school_year,
        is_primary_school,
        entry_date,
        calendar_date,
        course_code
),

daily_schedules as (
    /*
     * Collapse the schedule to one row per student-day while the record is
     * still narrow. The complete student-day is reattached afterward.
     */
    select
        k_student,
        k_school,
        k_school_calendar,
        school_year,
        is_primary_school,
        entry_date,
        calendar_date,

        sum(course_duration)
            as total_duration,

        sum(
            case
                when is_cte then course_duration
                else 0
            end
        ) as cte_duration,

        case
            when max(has_overlapping_periods) = 1 then 1
            else 0
        end as has_overlapping_periods,

        /*
         * A course is scheduled more than once when it produces multiple
         * meeting-time occurrences for this student-day.
         */
        case
            when max(scheduled_meeting_time_count) > 1 then 1
            else 0
        end as has_duplicate_course_scheduled,

        transform(
            sort_array(
                collect_list(course_info)
            ),

            course -> struct(
                course.course_code,
                course.scheduled_section_count,
                course.scheduled_meeting_time_count,
                course.course_duration,
                course.is_vocational_course,
                course.cte_cluster,

                course.fte_grade_weight,
                course.fte_grade_statutory_program,

                course.fte_sped_weight,
                course.fte_sped_statutory_program,

                course.fte_cte_weight,
                course.fte_cte_statutory_program,

                course.fteada_weight,
                course.fteada_program,

                course.meeting_times
            )
        ) as courses,

        max(tdoe_severity_code)
            as tdoe_severity_code

    from student_courses_aggregated

    group by
        k_student,
        k_school,
        k_school_calendar,
        school_year,
        is_primary_school,
        entry_date,
        calendar_date
),

student_daily_schedule as (
    /*
     * Reattach the complete student-day only after the schedule has been
     * reduced to one row per student-day.
     */
    select
        sm.k_student,
        sm.k_lea,
        sm.k_school,
        sm.k_school_calendar,
        sm.school_year,
        sm.is_primary_school,
        sm.entry_date,
        sm.exit_withdraw_date,
        sm.grade_level,
        sm.grade_level_adm,

        sm.fte_grade_weight,
        sm.fte_grade_statutory_program,
        sm.fte_sped_weight,
        sm.fte_sped_statutory_program,

        sm.is_early_graduate,
        sm.is_zeroday_early_graduate,
        sm.calendar_date,
        sm.isa_member,
        sm.is_sped,
        sm.is_funding_ineligible,
        sm.is_expelled,
        sm.is_suspended,
        sm.is_econdis,
        sm.is_el,
        sm.is_dyslexic,
        sm.is_absent,
        sm.is_early_grad_date,
        sm.ssd_duration,
        sm.report_period,
        sm.report_period_begin_date,
        sm.report_period_end_date,
        sm.school_days_in_report_period,

        schedules.total_duration,
        schedules.cte_duration,
        schedules.has_overlapping_periods,
        schedules.has_duplicate_course_scheduled,
        schedules.courses,
        schedules.tdoe_severity_code,

        {{ severity_code_to_severity_case_clause(
            'schedules.tdoe_severity_code'
        ) }}

    from {{ ref('student_days') }} sm

    join daily_schedules schedules
        on schedules.k_student = sm.k_student
        and schedules.k_school = sm.k_school
        and schedules.k_school_calendar = sm.k_school_calendar
        and schedules.school_year = sm.school_year
        and schedules.is_primary_school = sm.is_primary_school
        and schedules.entry_date = sm.entry_date
        and schedules.calendar_date = sm.calendar_date

    where sm.is_early_grad_date = 0
),

early_grad_student_days as (
    /*
     * Early Grad Dates do not have classes or period durations.
     */
    select
        sm.k_student,
        sm.k_lea,
        sm.k_school,
        sm.k_school_calendar,
        sm.school_year,
        sm.is_primary_school,
        sm.entry_date,
        sm.exit_withdraw_date,
        sm.grade_level,
        sm.grade_level_adm,

        sm.fte_grade_weight,
        sm.fte_grade_statutory_program,
        sm.fte_sped_weight,
        sm.fte_sped_statutory_program,

        sm.is_early_graduate,
        sm.is_zeroday_early_graduate,
        sm.calendar_date,
        sm.isa_member,
        sm.is_sped,
        sm.is_funding_ineligible,
        sm.is_expelled,
        sm.is_suspended,
        sm.is_econdis,
        sm.is_el,
        sm.is_dyslexic,
        sm.is_absent,
        sm.is_early_grad_date,
        sm.ssd_duration,
        sm.report_period,
        sm.report_period_begin_date,
        sm.report_period_end_date,
        sm.school_days_in_report_period,

        cast(null as double) as total_duration,
        cast(null as double) as cte_duration,

        0 as has_overlapping_periods,
        0 as has_duplicate_course_scheduled,

        null as courses,

        sm.tdoe_severity_code,
        sm.tdoe_severity

    from {{ ref('student_days') }} sm

    where sm.is_early_grad_date = 1
)

select * from early_grad_student_days
union all
select * from student_daily_schedule