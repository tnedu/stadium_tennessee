{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1006 %}

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
    where c.is_stockpile_day = true
),
sum_event_parts as (
    /* We need to sum up the event values. 
       But we cannot simply sum the fractional values because we could get rounding errors.
       So scale this to integers and then sum and scale back down to fractional values.
       We don't need to worry about multiple events being on the same day because there will
       be a different rule for that scenario. */
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
/* There cannot be more than 13 Stockpile days. */
select sep.k_school, sep.k_school_calendar, sep.school_year, sep.school_id, sep.calendar_code, 
    potential_tdoe_error_code as error_code,
    concat('Calendar ', sep.calendar_code, ' has total calculated Stockpile days more than the maximum of 13.0. Total days calculated: ',
        coalesce(sep.sum_event_values, 0), ' (', sep.count_events, ' events on ', count_days, ' days).') as error,
    {{ severity_to_severity_code_case_clause('potential_tdoe_severity') }},
    potential_tdoe_severity as tdoe_severity
from sum_event_parts sep
where coalesce(sep.sum_event_values, 0) > 13
order by 3, 4, 5