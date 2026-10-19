{{
  config(
    materialized = "table",
    schema = "build"
  )
}}
/* SSD is only counted on days the student has at least one class meeting, matching student_day_sections, which drops days without classes before fct_student_base_adm sums ssd_duration. */
with ssa_ssd_ranges as (
    select k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year, entry_date, exit_withdraw_date, effective_date, ssd_duration,
        lead(effective_date) over (partition by k_student, k_school, k_school_calendar, school_year, entry_date order by effective_date) as next_effective_date
    from {{ ref('bld_ef3__student_standard_days') }}
),
student_ssd as (
    select ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id, ssd.student_unique_id, ssd.school_year, ssd.entry_date, ssd.exit_withdraw_date,
        cd.report_period, cd.report_period_begin_date, cd.report_period_end_date, count(*) as ssd_school_days, sum(ssd.ssd_duration) as sum_ssd_mins
    from ssa_ssd_ranges ssd
    join {{ ref('bld_ef3__student_section_days') }} cd
        on cd.k_student = ssd.k_student
       and cd.k_school = ssd.k_school
       and cd.k_school_calendar = ssd.k_school_calendar
       and cd.entry_date = ssd.entry_date
       and cd.calendar_date >= ssd.effective_date
       and (ssd.next_effective_date is null or cd.calendar_date < ssd.next_effective_date)
    group by ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id, ssd.student_unique_id, ssd.school_year, ssd.entry_date, ssd.exit_withdraw_date,
        cd.report_period, cd.report_period_begin_date, cd.report_period_end_date
)
select * from student_ssd
