{{
  config(
    materialized = "table",
    schema = "build"
  )
}}

with ssa_ssd_ranges as (
    select k_student, k_school, k_school_calendar, school_id, student_unique_id, school_year,
        entry_date, exit_withdraw_date,
        effective_date as ssd_date_start,
        coalesce(
            date_add(
                lead(effective_date) over (
                    partition by k_student, k_school 
                    order by effective_date), 
                -1), 
                to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd')
            ) as ssd_date_end,
        ssd_duration
    from {{ ref('bld_ef3__student_standard_days') }}
),
student_ssd as (
    select ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id, ssd.entry_date,
        ssd.student_unique_id, ssd.school_year, rp.report_period, 
        rp.report_period_begin_date, rp.report_period_end_date,
        sum(ssd.ssd_duration) as sum_ssd_mins
    from ssa_ssd_ranges ssd
    join {{ ref('bld_ef3__calendar_report_periods') }} rp
        on rp.k_school_calendar = ssd.k_school_calendar
       and rp.calendar_date between ssd.ssd_date_start and ssd.ssd_date_end
       and rp.school_day_of_report_period is not null
    group by ssd.k_student, ssd.k_school, ssd.k_school_calendar, ssd.school_id, ssd.entry_date,
        ssd.student_unique_id, ssd.school_year, rp.report_period, 
        rp.report_period_begin_date, rp.report_period_end_date
)
select * from student_ssd