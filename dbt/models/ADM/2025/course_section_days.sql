{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get all the Course Sections and Days that meet that contribute to ADM, 
along with the duration of the Section (which can be split across multiple class periods).
Class Period Meeting Times have been added for detail in the next step.
*/

with course_sections as (
    /*
     * Determine the CTE FTE/ADA program at the Course Section grain before
     * expanding the Section across Class Period Dates.
     */
    select
        dcs.school_year,
        dcs.k_school,
        dcs.k_course_section,
        dcs.course_code,
        dcs.is_cte,
        dcs.cte_cluster,

        fteada.weight as fte_cte_weight,
        fteada.statutory_program as fte_cte_statutory_program,

        dcs.tdoe_severity_code

    from {{ ref('dim_course_section') }} dcs

    left join {{ ref('fteada_statutory_programs') }} fteada
        on fteada.program_grouping = 'CTE'
        and dcs.is_cte = true
        and fteada.program_criterion = coalesce(
            dcs.cte_cluster,
            'General CTE'
        )
        and dcs.school_year between
            fteada.start_school_year
            and coalesce(fteada.end_school_year, 9999)

    /* This syntax allows nulls and anything that is not 'P'. */
    where dcs.educational_environment_type is distinct from 'P'

        and not exists (
            select 1
            from {{ ref('ignored_adm_courses') }} ignored
            where ignored.course_code = dcs.course_code
                and dcs.school_year >= ignored.ignored_course_school_year_start
                and (
                    ignored.ignored_course_school_year_end is null
                    or dcs.school_year <= ignored.ignored_course_school_year_end
                )
        )
),

course_section_periods as (
    /*
     * Expand the eligible Course Sections across their Class Period Dates.
     * The CTE FTE/ADA determination has already been made above.
     */
    select
        cs.school_year,
        cs.k_school,
        cs.k_course_section,
        scpd.class_period_name,
        scpd.bell_schedule_name,
        cs.course_code,
        cs.is_cte,
        cs.cte_cluster,

        cs.fte_cte_weight,
        cs.fte_cte_statutory_program,

        scpd.calendar_date,
        scpd.start_time,
        scpd.end_time,
        scpd.period_duration,

        greatest(
            coalesce(cs.tdoe_severity_code, 0),
            coalesce(scpd.tdoe_severity_code, 0)
        ) as tdoe_severity_code

    from course_sections cs

    join {{ ref('fct_section_class_period_dates') }} scpd
        on scpd.school_year = cs.school_year
        and scpd.k_school = cs.k_school
        and scpd.k_course_section = cs.k_course_section
)

select
    *,
    {{ severity_code_to_severity_case_clause('tdoe_severity_code') }}
from course_section_periods
