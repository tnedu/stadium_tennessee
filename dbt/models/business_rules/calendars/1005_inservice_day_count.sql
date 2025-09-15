{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1005 %}

with calendars as (
    select *
    from {{ ref('stg_ef3__calendars') }} c
    where 1=1
        {{ school_year_exists(error_code, 'c') }}
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
    select k_school, k_school_calendar, school_year, school_id, calendar_code, count(*) as inservice_days
    from calendar_events
    where calendar_event in ('IS', 'IO')
    group by k_school, k_school_calendar, school_year, school_id, calendar_code
),
errors as (
    /* There must be at least 5 in service days. */
    select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
        {{ error_code }} as error_code,
        concat('Calculated total In-Service days is less than the minimum of 5. Total days calculated: ',
        ifnull(x.inservice_days,0), '.') as error
    from calendars c
    left outer join not_enough_dates x
        on x.k_school = c.k_school
        and x.k_school_calendar = c.k_school_calendar
    where ifnull(x.inservice_days, 0) < 5
    order by 3, 4, 5
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}