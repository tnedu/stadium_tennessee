{{
  config(
    materialized="table",
    schema="cds"
  )
}}

/* We want to create an array of behavior information in case someone wants to explode and join it to
   dim_behavior. And also, we want to know if any behavior on this student discipline action is 
   related to any tdoe zero tolerance behaviors. */

/* I stole a lot of this code from bld_ef3__stu_discipline_action_stu_incident_behavior.sql */

with stu_discipline_incident_behaviors_actions as (
    select * 
    from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
stu_discipline_incident_behavior as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
),
dim_behavior as (
    select * from {{ ref('dim_behavior') }}
),
joined as (
    select
        stu_discipline_incident_behaviors_actions.tenant_code,
        stu_discipline_incident_behaviors_actions.k_student,
        stu_discipline_incident_behaviors_actions.api_year,
        stu_discipline_incident_behaviors_actions.discipline_action_id,
        stu_discipline_incident_behaviors_actions.discipline_date,
        {{ dbt_utils.generate_surrogate_key(
            ['stu_discipline_incident_behaviors_actions.k_student',
            'stu_discipline_incident_behavior.k_discipline_incident',
            'lower(stu_discipline_incident_behavior.behavior_type)']
        ) }}
        as k_student_discipline_incident_behavior,
        stu_discipline_incident_behavior.behavior_type,
        dim_behavior.is_tdoe_zero_tolerance, dim_behavior.k_behavior
    from stu_discipline_incident_behaviors_actions
    join stu_discipline_incident_behavior
        on stu_discipline_incident_behaviors_actions.k_student = stu_discipline_incident_behavior.k_student
        and stu_discipline_incident_behaviors_actions.k_student_xyear = stu_discipline_incident_behavior.k_student_xyear
        and stu_discipline_incident_behaviors_actions.school_id = stu_discipline_incident_behavior.school_id
        and stu_discipline_incident_behaviors_actions.incident_id = stu_discipline_incident_behavior.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, '1') = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, '1', stu_discipline_incident_behavior.behavior_type)
    join dim_behavior
        on dim_behavior.behavior_code = stu_discipline_incident_behavior.behavior_type
        and dim_behavior.school_year = stu_discipline_incident_behavior.school_year
),
aggregated as (
    select tenant_code, k_student, api_year, discipline_action_id, discipline_date, 
        {{
            edu_edfi_source.json_array_agg(
                edu_edfi_source.json_object_construct(
                    [['k_student_discipline_incident_behavior','k_student_discipline_incident_behavior'],
                     ['behavior_type', 'behavior_type'],
                     ['is_tdoe_zero_tolerance', 'is_tdoe_zero_tolerance'],
                     ['k_behavior', 'k_behavior']]
                ),
            is_terminal=True)
        }} as tdoe_student_incident_behavior_array,
        max(is_tdoe_zero_tolerance) as has_tdoe_zero_tolerance_behavior
    from joined
    {{ dbt_utils.group_by(n=5) }}
)
select * from aggregated