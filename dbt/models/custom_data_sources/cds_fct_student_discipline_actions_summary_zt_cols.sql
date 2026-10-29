{{
  config(
    materialized="table",
    schema="cds"
  )
}}

/* We want to create an array of behavior information in case someone wants to explode and join it to
   dim_behavior. And also, we want to know if any behavior on this is 
   related to any tdoe zero tolerance behaviors. */

/* I stole a lot of this code from fct_student_discipline_actions_summary.sql */
with stu_discipline_incident_behaviors_actions as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
fct_student_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
dim_behavior as (
    select * from {{ ref('dim_behavior') }}
),
behaviors_array as (
    -- goal here is to find all behaviors for an action ID, regardless of whether they are linked to
    -- different incident IDs
    select
        stu_discipline_incident_behaviors_actions.k_student,
        stu_discipline_incident_behaviors_actions.discipline_action_id,
        stu_discipline_incident_behaviors_actions.discipline_date,
        {{
            edu_edfi_source.json_array_agg(
                edu_edfi_source.json_object_construct(
                     [['behavior_type', 'fct_student_discipline_incident_behaviors.behavior_type'],
                     ['is_tdoe_zero_tolerance', 'dim_behavior.is_tdoe_zero_tolerance'],
                     ['k_behavior', 'dim_behavior.k_behavior']]
                ),
            is_terminal=True)
        }} as tdoe_behavior_types_array,
        max(dim_behavior.is_tdoe_zero_tolerance) as has_tdoe_zero_tolerance_behavior
    from stu_discipline_incident_behaviors_actions
    join fct_student_discipline_incident_behaviors
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_incident_behaviors.k_student
        and stu_discipline_incident_behaviors_actions.k_student_xyear = fct_student_discipline_incident_behaviors.k_student_xyear
        and stu_discipline_incident_behaviors_actions.school_id = fct_student_discipline_incident_behaviors.school_id
        and stu_discipline_incident_behaviors_actions.incident_id = fct_student_discipline_incident_behaviors.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, '1') = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, '1', fct_student_discipline_incident_behaviors.behavior_type)
    join dim_behavior
        on dim_behavior.behavior_code = fct_student_discipline_incident_behaviors.behavior_type
        and dim_behavior.school_year = fct_student_discipline_incident_behaviors.school_year
    group by 1,2,3
)
select * from behaviors_array