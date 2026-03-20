{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1007 %}

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
    where c.is_parent_teacher_day = true
),
xwalk_calendar_events as (
    select *
    from {{ ref('xwalk_calendar_events') }}
    where is_parent_teacher_day = true
),
calendar_events as (
    select c.k_school, c.k_school_calendar, cd.k_calendar_date, c.tenant_code, c.api_year, c.school_year,
        c.school_id, c.calendar_code, cd.calendar_date, ce.calendar_event
    from calendars c
    left outer join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = c.k_school_calendar
    left outer join {{ ref('stg_ef3__calendar_dates__calendar_events') }} ce
        on ce.k_school_calendar = cd.k_school_calendar
        and ce.k_calendar_date = cd.k_calendar_date
),
not_enough_dates as (
    select k_school, k_school_calendar, school_year, school_id, calendar_code, count(*) as pt_days
    from calendar_events
    where calendar_event in (select calendar_event_descriptor from xwalk_calendar_events)
    group by k_school, k_school_calendar, school_year, school_id, calendar_code
),
errors as (
    /* There must be at least 1 in PT days. */
    select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    brule.tdoe_error_code as error_code,
    concat('Calendar ', c.calendar_code, ' has calculated total Parent-Teacher days is less than the minimum of 1. Total days calculated: ',
        ifnull(x.pt_days,0), '.') as error
    from calendars c
    left outer join not_enough_dates x
        on x.k_school = c.k_school
        and x.k_school_calendar = c.k_school_calendar
    join brule
        on c.school_year between brule.error_school_year_start and brule.error_school_year_end
    where ifnull(x.pt_days, 0) < 1
    order by 3, 4, 5
sum_event_parts as (
    /* We need to sum up the event values. 
       But we cannot simply sum the fractional values because we could get rounding errors.
       So scale this to integers and then sum and scale back down to fractional values. */
    select k_school, k_school_calendar, tenant_code, api_year, school_year, school_id, calendar_code, 
        potential_tdoe_error_code, potential_tdoe_severity,
        cast(
            sum(
                case pct_of_day
                    when 0.33 then 2
                    when 0.5 then 3
                    when 1 then 6
                end) / 6.0
        as decimal(10,2)) as sum_event_values,
        count(*) as count_events,
        count(distinct calendar_date) as count_days
    from specific_events
    group by k_school, k_school_calendar, tenant_code, api_year, school_year, school_id, calendar_code,
        potential_tdoe_error_code, potential_tdoe_severity
)
/* There must be at least 1 Parent/Teacher days. */
select sep.k_school, sep.k_school_calendar, sep.school_year, sep.school_id, sep.calendar_code, 
    potential_tdoe_error_code as error_code,
    concat('Calendar ', sep.calendar_code, ' has total calculated Parent/Teacher days less than the minimum of 1.0. Total days calculated: ',
        coalesce(sep.sum_event_values, 0), ' (', sep.count_events, ' events on ', count_days, ' days).') as error,
    {{ severity_to_severity_code_case_clause('potential_tdoe_severity') }},
    potential_tdoe_severity as tdoe_severity
from sum_event_parts sep
where coalesce(sep.sum_event_values, 0) < 1
order by 3, 4, 5