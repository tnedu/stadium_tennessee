{{
  config(
    materialized="table",
    schema="stg_validations",
    enabled=var('edu:data_validation', {}).get('enabled', false)
  )
}}

{% set error_code = 1004 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
calendars as (
    select *,
        brule.tdoe_error_code,
        brule.tdoe_severity
    from {{ ref('stg_ef3__calendars') }} c
    join brule
        on cast(c.school_year as int)
           between brule.error_school_year_start and brule.error_school_year_end
),
xwalk_calendar_events as (
    select *
    from {{ ref('xwalk_calendar_events') }}
    where is_required = true
),
calendar_events as (
    select c.*, cd.k_calendar_date, cd.calendar_date, ce.calendar_event
    from calendars c
    left outer join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = c.k_school_calendar
    left outer join {{ ref('stg_ef3__calendar_dates__calendar_events') }} ce
        on ce.k_school_calendar = cd.k_school_calendar
        and ce.k_calendar_date = cd.k_calendar_date
),
missing_events as (
    select k_school, k_school_calendar, school_year, school_id, calendar_code,
        substr(missing_calendar_events, 2, len(missing_calendar_events)-2) as missing_calendar_events
    from (
        select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, c.tdoe_error_code, c.tdoe_severity,
            cast(array_agg(re.required_calendar_event) as String) as missing_calendar_events
        from calendars c
        cross join (
            select distinct calendar_event_descriptor as required_calendar_event
            from xwalk_calendar_events
            where is_required = true
        )  re
        where not exists (
            select 1
            from calendar_events x
            where x.k_school = c.k_school
                and x.k_school_calendar = c.k_school_calendar
                and x.calendar_event = re.required_calendar_event
        )
        group by all
    )
),
validations as (
    /* Some Calendar Events are required on every calendar. */
    select c.k_school_calendar, 
        {{ edu_edfi_source.validation_struct(
            severity="c.tdoe_severity",
            code="c.tdoe_error_code",
            message="concat('Calendar ', c.calendar_code, ' is missing the following required events: ', x.missing_calendar_events, '.')"
        ) }} as validation
    from calendars c
    join missing_events x
        on x.k_school = c.k_school
        and x.k_school_calendar = c.k_school_calendar
)
select *
from validations