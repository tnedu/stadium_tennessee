{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get a Student's Classes by Day for ADM.
*/
with fteada as (
    select *
    from {{ ref('fteada_statutory_programs') }}
),
student_classes as (
    /* Get the Student Classes for Student Days for when the Day is not an Early Grad Date. 
    Early Grads Dates don't have classes, so we'll take care of them later. */
    select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
        sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
        sm.is_early_graduate, sm.calendar_date, sm.isa_member,
        sm.is_sped, sm.is_funding_ineligible, sm.is_expelled, sm.is_suspended, sm.is_EconDis, sm.is_EL, sm.is_Dyslexic, sm.is_absent,
        sm.is_early_grad_date,
        sm.ssd_duration,
        sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
        sm.days_in_report_period,
        si.course_code, si.start_time, si.end_time, coalesce(si.period_duration,0) as period_duration,
        si.is_cte, si.CTE_Cluster,  
        max_by(fteada.statutory_program, fteada.weight) as fteada_program,
        max(fteada.weight) as fteada_weight,
        greatest(coalesce(sm.tdoe_severity_code,0), coalesce(fssa.tdoe_severity_code,0), coalesce(si.tdoe_severity_code,0)) as tdoe_severity_code
    from {{ ref('student_days') }} sm
    join {{ ref('fct_student_section_association') }} fssa
        on fssa.school_year = sm.school_year
        and fssa.k_student = sm.k_student
        and fssa.k_school = sm.k_school
        /* The calendar date must be between the student's section dates. */
        and sm.calendar_date >= fssa.begin_date 
        and (fssa.end_date is null or sm.calendar_date <= fssa.end_date)
    join {{ ref('course_section_days') }} si
        on si.school_year = fssa.school_year
        and si.k_course_section = fssa.k_course_section
        and si.k_school = fssa.k_school
        and si.calendar_date = sm.calendar_date
    left outer join fteada 
        on sm.school_year between fteada.start_school_year and coalesce(fteada.end_school_year, 9999)
        and (
            (   /* Check for Grade matches */
                fteada.program_grouping = 'Grade'
                and fteada.program_criterion = sm.grade_level_adm
            )
            or (/* Check for SPED matches */
                fteada.program_grouping = 'Sped'
                and sm.is_sped = 1
            )
            or (/* Check for CTE matches */
                fteada.program_grouping = 'CTE'
                and si.is_cte = true
                and (
                    (si.CTE_Cluster is not null and fteada.program_criterion = si.CTE_Cluster)
                    or (si.CTE_Cluster is null and fteada.program_criterion = 'General CTE')
                )
            )
        )
    where sm.is_early_grad_date = 0
    group by sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
        sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
        sm.is_early_graduate, sm.calendar_date, sm.isa_member,
        sm.is_sped, sm.is_funding_ineligible, sm.is_expelled, sm.is_suspended, sm.is_EconDis, sm.is_EL, sm.is_Dyslexic, sm.is_absent,
        sm.is_early_grad_date,
        sm.ssd_duration,
        sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
        sm.days_in_report_period,
        si.course_code, si.start_time, si.end_time, coalesce(si.period_duration,0),
        si.is_cte, si.CTE_Cluster,
        greatest(coalesce(sm.tdoe_severity_code,0), coalesce(fssa.tdoe_severity_code,0), coalesce(si.tdoe_severity_code,0))
),
detect_overlaps as (
    select k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_suspended, is_EconDis, is_EL, is_Dyslexic, is_absent,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        course_code, start_time, end_time, period_duration,
        is_cte, CTE_Cluster, fteada_program, fteada_weight,
        case
            when end_time >
                lead(start_time) over (
                    partition by k_student, k_school, k_school_calendar, is_primary_school, entry_date, calendar_date
                    order by start_time, end_time)
                then 1
            else 0
        end as has_overlapping_period,
        tdoe_severity_code
    from student_classes
),
student_courses_aggregated as (
    select k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_suspended, is_EconDis, is_EL, is_Dyslexic, is_absent,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        course_code, 
        count(course_code) as section_count,
        sum(period_duration) as course_duration,
        max(is_cte) as is_cte,
        max(has_overlapping_period) as has_overlapping_periods,
        struct(
            min(start_time) as min_start_time,
            course_code,
            count(course_code) as section_count,
            sum(period_duration) as course_duration,
            max(
                case
                    when is_cte then 1
                    else 0
                end
            ) as is_vocational_course,
            max(CTE_Cluster) as CTE_Cluster,
            max(fteada_program) as fteada_program,
            max(fteada_weight) as fteada_weight,
            sort_array(collect_list(concat(start_time,'-',end_time))) as meeting_times
        ) AS course_info,
        max(tdoe_severity_code) as tdoe_severity_code
    from detect_overlaps
    group by k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_suspended, is_EconDis, is_EL, is_Dyslexic, is_absent,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        course_code
),
student_daily_schedule as (
    select k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_suspended, is_EconDis, is_EL, is_Dyslexic, is_absent,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        sum(course_duration) as total_duration,
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
        case
            when max(section_count) > 1 then 1
            else 0
        end as has_duplicate_course_scheduled,
        transform(
            sort_array(collect_list(course_info)),
            c -> struct(
                c.course_code,
                c.section_count,
                c.course_duration,
                c.is_vocational_course,
                c.CTE_Cluster,
                c.fteada_program,
                c.fteada_weight,
                c.meeting_times
            )
        ) as courses,
        max(tdoe_severity_code) as tdoe_severity_code,
        {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)') }}
    from student_courses_aggregated
    group by k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_suspended, is_EconDis, is_EL, is_Dyslexic, is_absent,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period
)
/* Here's the Early Grad Dates. They don't have classes, so they don't have period durations. */
select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
    sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
    sm.is_early_graduate, sm.calendar_date, sm.isa_member,
    is_sped, is_funding_ineligible, is_expelled, is_suspended, is_EconDis, is_EL, is_Dyslexic, is_absent,
    sm.is_early_grad_date,
    sm.ssd_duration,
    sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
    sm.days_in_report_period,
    null as total_duration, null as cte_duration, 
    0 as has_overlapping_periods, 0 as has_duplicate_course_scheduled,
    null as courses,
    tdoe_severity_code,
    tdoe_severity
from {{ ref('student_days') }} sm
where sm.is_early_grad_date = 1
union all
/* Here's the rest of the Dates, which have classes and period durations. */
select *
from student_daily_schedule