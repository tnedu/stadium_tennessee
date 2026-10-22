{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1000 %}

with brule as (
    select tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
specific_events as (
    /* Get the Calendars governed by this rule. */
    select distinct c.k_school, c.k_school_calendar, c.tenant_code, c.api_year, c.school_year, c.school_id, c.calendar_code, c.calendar_date, c.calendar_event, c.pct_of_day,
        case when c.is_statutory_day = true then 1 else 0 end as is_full_day_event,
        case when trim(split(c.calendar_event, ':')[0]) in ('SP_0.33', 'SP_0.5', 'SD_0.33', 'SD_0.5', 'SI_0.33', 'SI_0.5', 'SN_0.33', 'SN_0.5', 'SO_0.33', 'SO_0.5') then 1 else 0 end as is_partial_day_event,
        brule.tdoe_error_code as potential_tdoe_error_code,
        brule.tdoe_severity as potential_tdoe_severity
    from {{ ref('wrk_calendar_events') }} c
    join brule brule
        on cast(c.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
),
sum_day_parts as (
    select k_school, k_school_calendar, tenant_code, api_year, school_year, school_id, calendar_code, potential_tdoe_error_code, potential_tdoe_severity, calendar_date,
        max(case when is_full_day_event = 1 then 6 else 0 end)
        + sum(case when is_partial_day_event = 1 then case pct_of_day when 0.33 then 2 when 0.5 then 3 else 0 end else 0 end) as day_parts,
        sum(is_full_day_event + is_partial_day_event) as count_events
    from specific_events
    group by k_school, k_school_calendar, tenant_code, api_year, school_year, school_id, calendar_code, potential_tdoe_error_code, potential_tdoe_severity, calendar_date
),
sum_event_parts as (
    select k_school, k_school_calendar, tenant_code, api_year, school_year, school_id, calendar_code, potential_tdoe_error_code, potential_tdoe_severity,
        sum(day_parts) as sum_parts,
        cast(sum(day_parts) / 6.0 as decimal(10,2)) as sum_event_values,
        sum(count_events) as count_events,
        count(case when day_parts > 0 then calendar_date end) as count_days
    from sum_day_parts
    group by k_school, k_school_calendar, tenant_code, api_year, school_year, school_id, calendar_code, potential_tdoe_error_code, potential_tdoe_severity
)
/* There must be at least 180 instructional days. The check is done on the integer parts: 180 days = 180 * 6 = 1080 parts. */
select sep.k_school, sep.k_school_calendar, sep.school_year, sep.school_id, sep.calendar_code,
    potential_tdoe_error_code as error_code,
    concat('Calendar ', sep.calendar_code, ' has calculated total instructional days is less than the minimum of 180. Total days calculated: ',
        regexp_replace(cast(coalesce(sep.sum_event_values, 0) as string), '\\.?0+$', ''), '.') as error,
    {{ severity_to_severity_code_case_clause('potential_tdoe_severity') }},
    potential_tdoe_severity as tdoe_severity
from sum_event_parts sep
where coalesce(sep.sum_parts, 0) < 1080
order by 3, 4, 5
