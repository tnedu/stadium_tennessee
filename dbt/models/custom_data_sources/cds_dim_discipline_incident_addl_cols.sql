{{
  config(
    materialized="table",
    schema="cds"
  )
}}

/* We want to create an array of behavior information in case someone wants to explode and join it to
   dim_behavior. And also, we want to know if any behavior on this incident contains any tdoe
   zero tolerance behaviors. */

with stg_discipline_incidents as (
    select * from {{ ref('stg_ef3__discipline_incidents') }}
),
dim_behavior as (
    select * from {{ ref('dim_behavior') }}
),
behaviors as (
    select
        k_discipline_incident,
        {{
            edu_edfi_source.json_array_agg(
                edu_edfi_source.json_object_construct(
                    [['behavior_type', 'behaviors.behavior_type'],
                     ['is_tdoe_zero_tolerance', 'dim_behavior.is_tdoe_zero_tolerance'],
                     ['k_behavior', 'dim_behavior.k_behavior']]
                ),
            is_terminal=True)
        }} as tdoe_behavior_array,
        max(dim_behavior.is_tdoe_zero_tolerance) as has_tdoe_zero_tolerance_behavior
    from {{ ref('stg_ef3__discipline_incidents__behaviors') }} behaviors
    join dim_behavior
        on dim_behavior.behavior_code = behaviors.behavior_type
        and dim_behavior.school_year = behaviors.api_year
    group by k_discipline_incident
)
select * from behaviors