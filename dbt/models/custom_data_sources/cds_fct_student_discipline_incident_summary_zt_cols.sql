{{
  config(
    materialized="table",
    schema="cds"
  )
}}

/* We want to create an array of behavior information in case someone wants to explode and join it to
   dim_behavior. And also, we want to know if any behavior on this incident contains any tdoe
   zero tolerance behaviors. */

with stg_stu_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
aggregated as (
    select
        k_student,
        k_discipline_incident,
        {{
            edu_edfi_source.json_array_agg(
                edu_edfi_source.json_object_construct(
                    [['behavior_type','behavior_type'],
                     ['is_tdoe_zero_tolerance', 'is_tdoe_zero_tolerance'],
                     ['k_behavior', 'k_behavior']]
                ),
            is_terminal=True)
        }} as tdoe_student_incident_behavior_array,
        max(is_tdoe_zero_tolerance) as has_tdoe_zero_tolerance_behavior
    from stg_stu_discipline_incident_behaviors
    group by k_student, k_discipline_incident
)
select * from aggregated