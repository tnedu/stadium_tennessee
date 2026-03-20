{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

with unpivoted_coexistence_rules as (
    select perspective_calendar_event, other_calendar_event, coexistence_rule
    from {{ ref('calendar_event_coexistence_rules') }}
    unpivot include nulls (
        coexistence_rule
        for other_calendar_event in (
            CS, CE, 
            AE, AS, 
            TV, 
            OH, CH, SH, 
            MI, 
            ID, 
            MS, MD, MA, MU, MH, WN, 
            AD, `AD_0.33`, `AD_0.5`, 
            IO, `IO_0.33`, `IO_0.5`, IS, `IS_0.33`, `IS_0.5`, 
            OA, `OA_0.33`, `OA_0.5`, OI, `OI_0.33`, `OI_0.5`, OO, `OO_0.33`, `OO_0.5`, OS, `OS_0.33`, `OS_0.5`, OV, `OV_0.33`, `OV_0.5`, 
            PT, `PT_0.33`, `PT_0.5`, 
            SD, `SD_0.33`, `SD_0.5`, SI, `SI_0.33`, `SI_0.5`, SN, `SN_0.33`, `SN_0.5`, SO, `SO_0.33`, `SO_0.5`, SP, `SP_0.33`, `SP_0.5`, 
            WO
        )
    )
),
distinct_calendar_events as (
    select distinct calendar_event
    from {{ ref('stg_ef3__calendar_dates__calendar_events') }}
)
select perspective_event.calendar_event as perspective_calendar_event,
    other_event.calendar_event as other_calendar_event,
    rules.coexistence_rule
from unpivoted_coexistence_rules rules
join distinct_calendar_events perspective_event
    on perspective_event.calendar_event like concat(rules.perspective_calendar_event, ':%')
join distinct_calendar_events other_event
    on other_event.calendar_event like concat(rules.other_calendar_event, ':%')