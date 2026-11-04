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
        and rule_model = '{{ this.identifier }}'
),
calendars as (
    select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code,
        brule.tdoe_error_code,
        brule.tdoe_severity
    from {{ ref('stg_ef3__calendars') }} c
    join brule
        on cast(c.school_year as int)
           between brule.error_school_year_start and brule.error_school_year_end
),
specific_events as (
    select distinct c.k_school, c.k_school_calendar, c.calendar_date, c.calendar_event, c.pct_of_day
    from {{ ref('wrk_calendar_events') }} c
    where c.is_statutory_day = true
        and c.k_school_calendar in (select k_school_calendar from calendars)
),
sum_day_parts as (
    select k_school, k_school_calendar, calendar_date,
        sum(
            case pct_of_day
                when 0.33 then 2
                when 0.5 then 3
                when 1 then 6
            end) as day_parts
    from specific_events
    group by k_school, k_school_calendar, calendar_date
),
sum_event_parts as (
    select k_school, k_school_calendar, sum(day_parts) as sum_parts
    from sum_day_parts
    group by k_school, k_school_calendar
),
instructional_days as (
    /* Scale the integer parts back down to days, truncated to two decimal places. */
    select k_school, k_school_calendar,
        cast(floor(sum_parts * 100 / 6.0) / 100 as decimal(10,2)) as instructional_days
    from sum_event_parts
),
errors as (
    select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code,
        c.tdoe_error_code as error_code,
        concat('Calendar ', c.calendar_code, ' has calculated total instructional days is less than the minimum of 180. Total days calculated: ',
            coalesce(cast(x.instructional_days as string), '0.0'), '.') as error,
        {{ severity_to_severity_code_case_clause('c.tdoe_severity') }},
        c.tdoe_severity
    from calendars c
    left outer join instructional_days x
        on x.k_school = c.k_school
        and x.k_school_calendar = c.k_school_calendar
    where coalesce(x.instructional_days, 0) < 180
)
select *
from errors