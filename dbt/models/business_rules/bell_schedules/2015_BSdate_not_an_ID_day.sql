{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2015 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
bell_schedule_periods_dates as (
    select bs.k_bell_schedule, bs.k_school, bs.school_year, bs.tenant_code, bs.api_year,
        bs.bell_schedule_name, bs.school_id, bsd.calendar_date
    from {{ ref('stg_ef3__bell_schedules') }} bs
    join {{ ref('stg_ef3__bell_schedules__dates') }} bsd
        on bsd.tenant_code = bs.tenant_code
        and bsd.k_school = bs.k_school
        and bsd.k_bell_schedule = bs.k_bell_schedule
    where exists (
        select 1
        from brule
        where cast(bs.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
school_day_events as (
    select *
    from {{ ref('xwalk_calendar_events') }}
    where is_school_day = true
),
school_days as (
    select cal_dates.tenant_code, cal_dates.school_year, cal_dates.school_id,  cal_dates.calendar_date,
           cal_dates.calendar_code
    from {{ ref('stg_ef3__calendar_dates') }} cal_dates
    join {{ ref('stg_ef3__calendar_dates__calendar_events') }} cal_events
        on cal_events.api_year = cal_dates.api_year
        and cal_events.tenant_code = cal_dates.tenant_code
        and cal_events.k_calendar_date = cal_dates.k_calendar_date
    where cal_events.calendar_event in (select calendar_event_descriptor from school_day_events)
),
/* Bell Schedules dates must have ID event in school calendar. */
bsdates_missing_school_days as (
    select bsd.k_bell_schedule, bsd.school_year, bsd.k_school, bsd.school_id, 
            bsd.bell_schedule_name, bsd.calendar_date
    from bell_schedule_periods_dates bsd
    where not exists (
            select 1
            from school_days sd
            where sd.school_year = bsd.school_year
                and sd.school_id = bsd.school_id
                and sd.tenant_code = bsd.tenant_code
                and sd.calendar_date = bsd.calendar_date
        )
    group by bsd.k_bell_schedule, bsd.school_year, bsd.k_school, bsd.school_id, bsd.bell_schedule_name, 
             bsd.calendar_date
)
/*Generate Bell Schedule Date errors for all School Calendar Codes for dates that are not ID. */
select bs.k_bell_schedule, cast(bs.school_year as int) as school_year, bs.bell_schedule_name, bs.school_id,
        brule.tdoe_error_code as error_code,
        concat('Bell Schedule Date ', bs.calendar_date, ' in Bell Schedule ', bs.bell_schedule_name,  
        ' is not an instructional day in School ', bs.school_id, '.') as error,
        {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
        brule.tdoe_severity
from bsdates_missing_school_days bs
join brule
    on bs.school_year between brule.error_school_year_start and brule.error_school_year_end