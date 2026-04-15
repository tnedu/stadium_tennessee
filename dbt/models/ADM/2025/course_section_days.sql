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

select dcs.school_year, dcs.k_school, dcs.k_course_section, scpd.class_period_name, scpd.bell_schedule_name,
    dcs.course_code, dcs.is_cte, dcs.CTE_Cluster,
    scpd.calendar_date,
    scpd.start_time, scpd.end_time,
    scpd.period_duration as period_duration,
    greatest(coalesce(dcs.tdoe_severity_code, 0), coalesce(scpd.tdoe_severity_code,0)) as tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('greatest(coalesce(dcs.tdoe_severity_code, 0), coalesce(scpd.tdoe_severity_code,0))') }}
from {{ ref('dim_course_section') }} dcs
join {{ ref('fct_section_class_period_dates') }} scpd
    on scpd.school_year = dcs.school_year
    and scpd.k_school = dcs.k_school
    and scpd.k_course_section = dcs.k_course_section
where ifnull(dcs.educational_environment_type,'X') != 'P' /* Remove pull out classes. */
    and not exists (
        select 1
        from {{ ref('ignored_adm_courses') }} ignored
        where ignored.course_code = dcs.course_code
            and dcs.school_year >= ignored.ignored_course_school_year_start
            and (ignored.ignored_course_school_year_end is null or dcs.school_year <= ignored.ignored_course_school_year_end)
    )