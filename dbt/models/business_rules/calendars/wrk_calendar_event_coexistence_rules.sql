{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

with coexistence_rules_casted as (
    select perspective_calendar_event, school_year_start, school_year_end,
        cast(CS as string) as CS, cast(CE as string) as CE, 
        cast(AE as string) as AE, cast(AS as string) as AS, 
        cast(TV as string) as TV, 
        cast(OH as string) as OH, cast(CH as string) as CH, cast(SH as string) as SH, 
        cast(MI as string) as MI, 
        cast(ID as string) as ID, 
        cast(MS as string) as MS, cast(MD as string) as MD, cast(MA as string) as MA, cast(MU as string) as MU, cast(MH as string) as MH,
        cast(AD as string) as AD, cast(`AD_0.33` as string) as `AD_0.33`, cast(`AD_0.5` as string) as `AD_0.5`, 
        cast(IO as string) as IO, cast(`IO_0.33` as string) as `IO_0.33`, cast(`IO_0.5` as string) as `IO_0.5`, 
        cast(IS as string) as IS, cast(`IS_0.33` as string) as `IS_0.33`, cast(`IS_0.5` as string) as `IS_0.5`, 
        cast(OA as string) as OA, cast(`OA_0.33` as string) as `OA_0.33`, cast(`OA_0.5` as string) as `OA_0.5`, 
        cast(OI as string) as OI, cast(`OI_0.33` as string) as `OI_0.33`, cast(`OI_0.5` as string) as `OI_0.5`, 
        cast(OO as string) as OO, cast(`OO_0.33` as string) as `OO_0.33`, cast(`OO_0.5` as string) as `OO_0.5`, 
        cast(OS as string) as OS, cast(`OS_0.33` as string) as `OS_0.33`, cast(`OS_0.5` as string) as `OS_0.5`, 
        cast(OV as string) as OV, cast(`OV_0.33` as string) as `OV_0.33`, cast(`OV_0.5` as string) as `OV_0.5`, 
        cast(PT as string) as PT, cast(`PT_0.33` as string) as `PT_0.33`, cast(`PT_0.5` as string) as `PT_0.5`, 
        cast(SD as string) as SD, cast(`SD_0.33` as string) as `SD_0.33`, cast(`SD_0.5` as string) as `SD_0.5`, 
        cast(SI as string) as SI, cast(`SI_0.33` as string) as `SI_0.33`, cast(`SI_0.5` as string) as `SI_0.5`, 
        cast(SN as string) as SN, cast(`SN_0.33` as string) as `SN_0.33`, cast(`SN_0.5` as string) as `SN_0.5`, 
        cast(SO as string) as SO, cast(`SO_0.33` as string) as `SO_0.33`, cast(`SO_0.5` as string) as `SO_0.5`, 
        cast(SP as string) as SP, cast(`SP_0.33` as string) as `SP_0.33`, cast(`SP_0.5` as string) as `SP_0.5`, 
        cast(WN as string) as WN, cast(WO as string) as WO
    from {{ ref('calendar_event_coexistence_rules') }}
),
unpivoted_coexistence_rules as (
    select perspective_calendar_event, school_year_start, school_year_end, other_calendar_event, coexistence_rule
    from coexistence_rules_casted
    unpivot include nulls (
        coexistence_rule
        for other_calendar_event in (
            CS, CE, 
            AE, AS, 
            TV, 
            OH, CH, SH, 
            MI, 
            ID, 
            MS, MD, MA, MU, MH,
            AD, `AD_0.33`, `AD_0.5`, 
            IO, `IO_0.33`, `IO_0.5`, IS, `IS_0.33`, `IS_0.5`, 
            OA, `OA_0.33`, `OA_0.5`, OI, `OI_0.33`, `OI_0.5`, OO, `OO_0.33`, `OO_0.5`, OS, `OS_0.33`, `OS_0.5`, OV, `OV_0.33`, `OV_0.5`, 
            PT, `PT_0.33`, `PT_0.5`, 
            SD, `SD_0.33`, `SD_0.5`, SI, `SI_0.33`, `SI_0.5`, SN, `SN_0.33`, `SN_0.5`, SO, `SO_0.33`, `SO_0.5`, SP, `SP_0.33`, `SP_0.5`, 
            WN, WO
        )
    )
),
distinct_calendar_events as (
    select distinct calendar_event
    from {{ ref('stg_ef3__calendar_dates__calendar_events') }}
),
all_coexistence_rules as (
    select rules.school_year_start, rules.school_year_end,
        perspective_event.calendar_event as perspective_calendar_event,
        other_event.calendar_event as other_calendar_event,
        rules.coexistence_rule
    from unpivoted_coexistence_rules rules
    join distinct_calendar_events perspective_event
        on perspective_event.calendar_event like concat(rules.perspective_calendar_event, ':%')
    join distinct_calendar_events other_event
        on other_event.calendar_event like concat(rules.other_calendar_event, ':%')
),
canonical_forbidden_rules as (
    /* The reasoning here is that suppose you have a rule where ID is FORBIDDEN with SP. 
       In the sheet that governs the coexistence rules, you'd likely have FORBIDDEN on the ID row with the SP
       column and FORBIDDEN on the SP row with the ID column. All this is doing is creating two rules where 
       ID can't have an SP and where SP can't have an ID. So this basically duplicate rule triggers twice.
       Let's remove one of the rules when that happens so there's less issues to look at. */
    select distinct 
        school_year_start, school_year_end,
        least(perspective_calendar_event, other_calendar_event) as perspective_calendar_event,
        greatest(perspective_calendar_event, other_calendar_event) as other_calendar_event,
        coexistence_rule
    from all_coexistence_rules
    where coexistence_rule like 'FORBIDDEN%'
),
minimal_coexistence_rules as (
    /* These are the deduped FORBIDDEN rules. */
    select *
    from canonical_forbidden_rules
    union
    /* These are everything else. */
    select *
    from all_coexistence_rules
    where coexistence_rule not like 'FORBIDDEN%'
)
select *
from minimal_coexistence_rules