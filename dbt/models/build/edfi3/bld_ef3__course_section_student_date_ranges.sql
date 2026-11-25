{{
  config(
    materialized="table",
    schema="build"
  )
}}

with student_course_section_schedules as (
    /*
     * One row per student-school enrollment episode x
     * StudentSectionAssociation.
     */
    select
        k_course_section,
        tenant_code,
        school_year,
        effective_begin_date,
        effective_end_date
    from {{ ref('fct_student_course_section_schedule') }}
),

section_student_intervals as (
    /*
     * Remove duplicate intervals. We only care whether at least one student
     * provides coverage, not which student or how many students do so.
     *
     * Null end dates are temporarily represented by a stable upper bound
     * so they can participate in interval calculations.
     */
    select distinct
        k_course_section,
        tenant_code,
        school_year,
        effective_begin_date,
        coalesce(
            effective_end_date,
            date('9999-12-31')
        ) as effective_end_date_bound
    from student_course_section_schedules
    where effective_begin_date is not null
),
interval_running_end as (
    /*
     * Find the latest ending date among all preceding intervals for the
     * course section.
     *
     * Using the running maximum, rather than lag(end_date), correctly
     * handles nested intervals.
     */
    select
        *,
        max(effective_end_date_bound) over (
            partition by
                k_course_section,
                tenant_code,
                school_year
            order by
                effective_begin_date,
                effective_end_date_bound
            rows between unbounded preceding and 1 preceding
        ) as previous_max_effective_end_date
    from section_student_intervals
),
interval_groups as (
    /*
     * Start a new group only when there is at least one uncovered day
     * between the existing interval union and the next interval.
     *
     * Adjacent intervals are merged:
     *
     *   Aug 1-Aug 15
     *   Aug 16-Sep 30
     *
     * become:
     *
     *   Aug 1-Sep 30
     */
    select
        *,
        sum(
            case
                when previous_max_effective_end_date is null
                    or effective_begin_date > dateadd(
                        day,
                        1,
                        previous_max_effective_end_date
                    )
                then 1
                else 0
            end
        ) over (
            partition by
                k_course_section,
                tenant_code,
                school_year

            order by
                effective_begin_date,
                effective_end_date_bound

            rows between unbounded preceding and current row
        ) as interval_group
    from interval_running_end
),
merged_ranges as (
    /*
     * Grain:
     * One course section x continuous student-coverage range.
     */
    select
        k_course_section,
        tenant_code,
        school_year,
        interval_group,
        min(effective_begin_date) as student_coverage_begin_date,
        max(effective_end_date_bound) as student_coverage_end_date_bound
    from interval_groups
    group by
        k_course_section,
        tenant_code,
        school_year,
        interval_group
),
formatted as (
    select
        k_course_section,
        tenant_code,
        school_year,
        student_coverage_begin_date,
        case
            when student_coverage_end_date_bound = date('9999-12-31')
            then null
            else student_coverage_end_date_bound
        end as student_coverage_end_date
    from merged_ranges
)
select * from formatted