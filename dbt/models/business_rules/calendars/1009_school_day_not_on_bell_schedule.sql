{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1009 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
specific_events as (
    /* Get the Calendars governed by this rule. */
    select c.*,
        brule.tdoe_error_code as potential_tdoe_error_code,
        brule.tdoe_severity as potential_tdoe_severity
    from {{ ref('wrk_calendar_events') }} c
    join brule brule
        on cast(c.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where c.is_school_day = true
),
bell_schedule_periods_dates as (
    select bs.k_bell_schedule, bs.k_school, bs.school_year, bs.tenant_code, bs.api_year,
        bs.bell_schedule_name, bs.school_id, bsd.calendar_date
    from {{ ref('stg_ef3__bell_schedules') }} bs
    join {{ ref('stg_ef3__bell_schedules__dates') }} bsd
        on bsd.tenant_code = bs.tenant_code
        and bsd.k_school = bs.k_school
        and bsd.k_bell_schedule = bs.k_bell_schedule
)
/* A Bell Schedule Date MUST exist for every School Day. */
select se.k_school, se.k_school_calendar, se.school_year, se.school_id, se.calendar_code, 
    potential_tdoe_error_code as error_code,
    concat('Calendar ', se.calendar_code, ' has ', se.calendar_date , ' set as an Instructional Day but is not found on any Bell Schedule.') as error,
    {{ severity_to_severity_code_case_clause('potential_tdoe_severity') }},
    potential_tdoe_severity as tdoe_severity
from specific_events se
where not exists (
        select 1
        from bell_schedule_periods_dates bsd
        where se.school_year = bsd.school_year
            and se.school_id = bsd.school_id
            and se.tenant_code = bsd.tenant_code
            and se.calendar_date = bsd.calendar_date
    )