{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* This calculates the student membership for vocational classes. This is separate from
    student_membership because in this model, we need to keep the course code.
*/
with cte_courses_exploded as (
    select sds.*,
        course.course_code, course.course_duration, course.CTE_Cluster
    from {{ ref('student_day_sections') }} sds
    lateral view explode(sds.courses) as course
    where coalesce(sds.cte_duration,0) > 0
        and course.is_vocational_course = 1
)
select k_student, k_lea, k_school, k_school_calendar, school_year, is_primary_school, entry_date,
    exit_withdraw_date, grade_level, grade_level_adm, is_early_graduate, 
    calendar_date, isa_member, is_sped, is_funding_ineligible, is_expelled, is_EconDis,
    is_absent, is_early_grad_date,
    ssd_duration, report_period, report_period_begin_date, report_period_end_date,
    days_in_report_period,
    total_duration, cte_duration, has_overlapping_periods, has_duplicate_course_scheduled,
    course_code, 
    case
        when course_duration is null then 0
        when isa_member = 1 then course_duration
        else 0
    end as voc_class_duration,
    cast(
        (floor(
            (case
                --todo: should is_early_grad_date be 1 or 0? 
                when is_early_grad_date = 1 then 1
                when ssd_duration is null or ssd_duration = 0 then 0
                when course_duration is null then 0
                when isa_member = 1 then
                    cast(course_duration as decimal(12,8)) / cast(ssd_duration as decimal(12,8))
                else 0
            end) * 100000.0) / 100000.0)
        as decimal(8,5)
    ) as voc_membership,
    tdoe_severity_code as tdoe_severity_code,
    tdoe_severity
from cte_courses_exploded