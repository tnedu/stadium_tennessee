{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3302 %}

/* A Student's SSD must cover their entire enrollment period. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity,
        rule_model
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
    and rule_model = '{{this.identifier}}'
),
attendance_events as (
    select 
        ssae.*
    from {{ ref('stg_ef3__student_school_attendance_events') }} ssae
    join brule brule
        on cast(ssae.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where ssae.attendance_event_category = 'Student Standard Day'
),
first_ssd_per_student as (
    select k_student, k_school, cast(school_year as int) as school_year,
        min(attendance_event_date) as attendance_event_date
    from attendance_events 
    group by k_student, k_school, cast(school_year as int)
),
calendar_dates as (
    select cd.k_calendar_date, cd.k_school_calendar, c.k_school, cd.tenant_code,
        cd.school_year, cd.calendar_date, summarize_calendar_events.is_school_day
    from {{ ref('stg_ef3__calendar_dates') }} cd
    join {{ ref('stg_ef3__calendars') }} c
        on cd.k_school_calendar = c.k_school_calendar
    join (
            select 
                ce.k_calendar_date,
                -- if there are multiple events on a day, having at least one 
                -- that counts as a school day applies to the whole day
                sum(xce.is_school_day::integer) >= 1 as is_school_day
            from {{ ref('stg_ef3__calendar_dates__calendar_events') }} ce
            join {{ ref('xwalk_calendar_events') }} xce
                on ce.calendar_event = xce.calendar_event_descriptor
            group by 1
        ) summarize_calendar_events
        on cd.k_calendar_date = summarize_calendar_events.k_calendar_date
    where summarize_calendar_events.is_school_day = true
),
valid_enrollents_minus_zeroday_early_grads as (
    select *
    from {{ ref('valid_enrollments') }}
    where is_zeroday_early_graduate = 0
),
enrollments_and_ssd_date as (
    select ssa.k_student, ssa.k_school, ssa.school_year, ssa.school_id,
        ssa.student_unique_id, ssa.entry_date, ssa.exit_withdraw_date,
        fssd.attendance_event_date, brule.tdoe_error_code, brule.tdoe_severity,
        case
            when fssd.attendance_event_date is null then 0
            when fssd.attendance_event_date > ssa.entry_date then 0
            else 1
        end as ssd_good
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    /* fssd is  aleft join and errors are ssd =0 so joining brule to populate error code and severity. */
    join brule brule
        on cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    left outer join first_ssd_per_student fssd
        on fssd.k_school = ssa.k_school
        and fssd.k_student = ssa.k_student
        and fssd.school_year = ssa.school_year
    where 
        /* We only want this rule to fire if there exists an enrollment that is non-zero-day early grad. */
        exists (
            select 1
            from valid_enrollents_minus_zeroday_early_grads x
            where ssa.k_student = x.k_student
                and ssa.k_school = x.k_school
                and ssa.k_school_calendar = x.k_school_calendar
                and ssa.entry_date = x.entry_date
                and ssa.is_primary_school = x.is_primary_school
        )
),
errors as (
    select x.k_student, x.k_school, cast(null as string) as k_session, x.school_year,
        cast(x.school_id as int) as school_id, x.student_unique_id, 
        cast(null as date) as attendance_event_date, 'SSD' as attendance_event_category,
        s.state_student_id as legacy_state_student_id,
        x.tdoe_error_code as error_code,
        concat('Student Standard Day missing for Student: ', x.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]') ,'), ',
            'District: ', {{ get_district_from_school_id('x.school_id') }}, ', ',
            'School: ', x.school_id, ', ',
            'Enrollment Entry Date: ', x.entry_date, ', ',
            'Enrollment End Date: ', coalesce(x.exit_withdraw_date, '[null]'), ', ',
            'First SSD Date: ', coalesce(x.attendance_event_date, '[null]'), '.') as error,
        {{ severity_to_severity_code_case_clause('x.tdoe_severity') }},
        x.tdoe_severity
    from enrollments_and_ssd_date x
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = x.k_student
    where x.ssd_good = 0
)
select * from errors