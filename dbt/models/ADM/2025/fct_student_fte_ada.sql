{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_school_calendar set not null",
        "alter table {{ this }} alter column report_period set not null",
        "alter table {{ this }} alter column is_primary_school set not null",
        "alter table {{ this }} add primary key (k_student, k_lea, k_school, k_school_calendar, is_primary_school, report_period)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('edu_wh', 'dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('edu_wh', 'dim_lea') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('edu_wh', 'dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school_calendar foreign key (k_school_calendar) references {{ ref('edu_wh', 'dim_school_calendar') }}"
    ]
  )
}}

with student_fteada as (
    select sds.k_student, sds.k_school, sds.k_lea, sds.k_school_calendar, sds.school_year, sds.is_primary_school, sds.entry_date, sds.calendar_date, sds.is_early_grad_date, 
        sds.isa_member, sds.is_funding_ineligible, sds.is_absent, sds.is_suspended, sds.ssd_duration, sds.report_period, sds.report_period_begin_date, 
        sds.report_period_end_date, sds.days_in_report_period,
        course.course_duration, course.fteada_program, course.fteada_weight
    from {{ ref('student_day_sections') }} sds
    lateral view outer explode(sds.courses) as course
),
grouped_by_program as (
    select k_student, k_school, k_lea, k_school_calendar, school_year, is_primary_school, entry_date, calendar_date, is_early_grad_date, 
        isa_member, is_funding_ineligible, is_absent, is_suspended, ssd_duration, report_period, report_period_begin_date, 
        report_period_end_date, days_in_report_period,
        sum(course_duration) as program_duration, 
        fteada_program, fteada_weight
    from student_fteada
    group by all
),
daily_attendance as (
    select k_student, k_school, k_lea, k_school_calendar, school_year, is_primary_school, entry_date, calendar_date, is_early_grad_date, 
        isa_member, is_funding_ineligible, is_absent, is_suspended, ssd_duration, report_period, report_period_begin_date, 
        report_period_end_date, days_in_report_period,
        case
            when isa_member = 0 or is_absent = 1 or is_suspended = 1 then 0
            when coalesce(ssd_duration, 0) = 0 then 0
            when coalesce(program_duration, 0) = 0 then 0
            else cast((floor(cast(program_duration as decimal(12,8)) / cast(ssd_duration as decimal(12,8)) * 100000.0) / 100000.0) as decimal(8,5))
        end as daily_attendance,
        case
            when isa_member = 0 then 0
            when coalesce(ssd_duration, 0) = 0 then 0
            when coalesce(program_duration, 0) = 0 then 0
            else cast((floor(cast(program_duration as decimal(12,8)) / cast(ssd_duration as decimal(12,8)) * 100000.0) / 100000.0) as decimal(8,5))
        end as daily_attendance_ifa_member,
        program_duration, 
        fteada_program, fteada_weight,
        date_format(calendar_date, 'E') as day_of_week
    from grouped_by_program
),
fake_schedule_by_dow as (
    /* For EGs, we need to get the latest day for each day of the week, for which the student is not yet an EG. 
    We do this so that we can fake the EG days with actual minutes since EGs should continue to generate funding. */
    select k_student, k_school, k_lea, k_school_calendar, school_year, is_primary_school, entry_date, day_of_week, 
        daily_attendance_ifa_member as daily_attendance, program_duration, fteada_program, fteada_weight, calendar_date
    from daily_attendance
    where is_early_grad_date = 0
        and isa_member = 1
    qualify 1 =
        dense_rank() over (
            partition by k_student, k_school, k_lea, k_school_calendar, school_year, is_primary_school, entry_date, day_of_week
            order by calendar_date desc
        )
),
unioned_eg_schedule as (
    select k_student, k_school, k_lea, k_school_calendar, school_year, is_primary_school, entry_date, calendar_date, is_early_grad_date, 
        isa_member, is_funding_ineligible, is_absent, is_suspended, ssd_duration, report_period, report_period_begin_date, 
        report_period_end_date, days_in_report_period, daily_attendance,
        program_duration, fteada_program, fteada_weight
    from daily_attendance
    where is_early_grad_date = 0
    union all
    /* Now for the EG dates, we join in the fake schedule we made above. */
    select da.k_student, da.k_school, da.k_lea, da.k_school_calendar, da.school_year, da.is_primary_school, da.entry_date, da.calendar_date, da.is_early_grad_date, 
        da.isa_member, da.is_funding_ineligible, da.is_absent, da.is_suspended, da.ssd_duration, da.report_period, da.report_period_begin_date, 
        da.report_period_end_date, da.days_in_report_period, fs.daily_attendance,
        fs.program_duration, fs.fteada_program, fs.fteada_weight
    from daily_attendance da
    left outer join fake_schedule_by_dow fs
        on fs.k_student = da.k_student
        and fs.k_school = da.k_school
        and fs.k_school_calendar = da.k_school_calendar
        and fs.is_primary_school = da.is_primary_school
        and fs.entry_date = da.entry_date
        and fs.day_of_week = da.day_of_week
    where is_early_grad_date = 1
),
fteada as (
    select k_student, k_school, k_lea, k_school_calendar, school_year, is_primary_school,  
        sum(is_early_grad_date) as early_grad_days, 
        sum(isa_member) member_days, 
        sum(is_funding_ineligible) as funding_ineligible_days,
        sum(is_absent) as absent_days, 
        sum(is_suspended) as suspended_days, 
        report_period, report_period_begin_date, 
        report_period_end_date, days_in_report_period,
        sum(ssd_duration) as sum_ssd_duration, 
        sum(program_duration) as sum_program_duration,
        cast(
            (floor(
                (case
                    when coalesce(days_in_report_period,0) = 0 then 0
                    when coalesce(sum(daily_attendance),0) = 0 then 0
                    else sum(daily_attendance) / cast(days_in_report_period as decimal(12,8))
                end) * 100000) / 100000)
            as decimal(8,5)
        ) as actual_fteada,
        cast(
            (floor(
                (case
                    when coalesce(days_in_report_period,0) = 0 then 0
                    when coalesce(sum(daily_attendance),0) = 0 then 0
                    else least( sum(least(daily_attendance, 1.0)) / cast(least(days_in_report_period,20) as decimal(12,8)), 1.0 )
                end) * 100000) / 100000)
            as decimal(8,5)
        ) as normalized_fteada,
        fteada_program, fteada_weight
    from unioned_eg_schedule
    group by all
)
select *,
    cast((floor(normalized_fteada * fteada_weight * 100000) / 100000) as decimal(8,5)) as normalized_fteada_weighted
from fteada
order by k_lea, k_school, is_primary_school, k_student, report_period, fteada_program