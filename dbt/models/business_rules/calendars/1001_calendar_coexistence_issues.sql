{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1001 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
multi_event_days as (
    select c.*,
        brule.tdoe_error_code as potential_tdoe_error_code
    from {{ ref('wrk_calendar_events') }} c
    join brule brule
        on cast(c.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where n_calendar_events > 1
),
coexistence_rules as (
    select *,
        lower(split(coexistence_rule, ':')[1]) as potential_tdoe_severity
    from {{ ref('wrk_calendar_event_coexistence_rules') }}
    where coalesce(coexistence_rule, 'IGNORE') not in ('IGNORE', 'ALLOWED')
),
forbidden_event_conflicts as (
    select perspective_events.*,
        rules.other_calendar_event, rules.coexistence_rule, rules.potential_tdoe_severity
    from multi_event_days perspective_events
    join coexistence_rules rules
        on rules.perspective_calendar_event = perspective_events.calendar_event
    where rules.coexistence_rule like 'FORBIDDEN%'
        and perspective_events.school_year between rules.school_year_start and coalesce(rules.school_year_end, 9999)
        and exists (
            select 1
            from multi_event_days other_events
            where other_events.k_school_calendar = perspective_events.k_school_calendar
                and other_events.k_school = perspective_events.k_school
                and other_events.tenant_code = perspective_events.tenant_code
                and other_events.school_year = perspective_events.school_year
                and other_events.calendar_code = perspective_events.calendar_code
                and other_events.k_calendar_date = perspective_events.k_calendar_date
                and other_events.calendar_date = perspective_events.calendar_date
                and other_events.calendar_event = rules.other_calendar_event
        )
),
required_event_conflicts as (
    select perspective_events.*,
        rules.other_calendar_event, rules.coexistence_rule, rules.potential_tdoe_severity
    from multi_event_days perspective_events
    join coexistence_rules rules
        on rules.perspective_calendar_event = perspective_events.calendar_event
    where rules.coexistence_rule like 'REQUIRED%'
        and perspective_events.school_year between rules.school_year_start and coalesce(rules.school_year_end, 9999)
        and not exists (
            select 1
            from multi_event_days other_events
            where other_events.k_school_calendar = perspective_events.k_school_calendar
                and other_events.k_school = perspective_events.k_school
                and other_events.tenant_code = perspective_events.tenant_code
                and other_events.school_year = perspective_events.school_year
                and other_events.calendar_code = perspective_events.calendar_code
                and other_events.k_calendar_date = perspective_events.k_calendar_date
                and other_events.calendar_date = perspective_events.calendar_date
                and other_events.calendar_event = rules.other_calendar_event
        )
)
select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    c.potential_tdoe_error_code as error_code,
    concat('Calendar ', c.calendar_code, ' has Calendar Event "', c.calendar_event, '" on ', c.calendar_date, ' and cannot also have Calendar Event "', c.other_calendar_event, '" on the same date.') as error,
    {{ severity_to_severity_code_case_clause('c.potential_tdoe_severity') }},
    c.potential_tdoe_severity as tdoe_severity
from forbidden_event_conflicts c
union
select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    c.potential_tdoe_error_code as error_code,
    concat('Calendar ', c.calendar_code, ' has Calendar Event "', c.calendar_event, '" on ', c.calendar_date, ' and must also have Calendar Event "', c.other_calendar_event, '" on the same date.') as error,
    {{ severity_to_severity_code_case_clause('c.potential_tdoe_severity') }},
    c.potential_tdoe_severity as tdoe_severity
from required_event_conflicts c