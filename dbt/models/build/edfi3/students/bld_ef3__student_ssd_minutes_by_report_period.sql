{{
  config(
    materialized = "table",
    schema = "build"
  )
}}

with ssa_ssd_ranges as (
    select k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year,
        entry_date, exit_withdraw_date, effective_date,
        lead(effective_date) over (
            partition by k_student, k_school, k_school_calendar, school_id, school_year
            order by effective_date
        ) as next_effective_date,
        ssd_duration
    from {{ ref('bld_ef3__student_standard_days') }}
),
student_ssd as (
    select ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id,
        ssd.student_unique_id, ssd.school_year, ssd.entry_date, rp.report_period,
        sum(ssd.ssd_duration) as sum_ssd_mins
    from ssa_ssd_ranges ssd
    join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = ssd.k_school_calendar
       and cd.calendar_date >= greatest(ssd.effective_date, ssd.entry_date)
       and (ssd.next_effective_date is null or cd.calendar_date < ssd.next_effective_date)
       and (ssd.exit_withdraw_date is null or cd.calendar_date <= ssd.exit_withdraw_date)
    join {{ ref('bld_ef3__calendar_report_periods') }} rp
        on rp.k_calendar_date = cd.k_calendar_date
    group by ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id,
        ssd.student_unique_id, ssd.school_year, ssd.entry_date, rp.report_period
)
select * from student_ssd