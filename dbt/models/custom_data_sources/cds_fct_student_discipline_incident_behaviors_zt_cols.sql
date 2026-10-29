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
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
),
dim_behavior as (
    select * from {{ ref('dim_behavior') }}
),
behaviors as (
    select
        stg_stu_discipline_incident_behaviors.k_student,
        stg_stu_discipline_incident_behaviors.k_discipline_incident,
        lower(stg_stu_discipline_incident_behaviors.behavior_type) as lower_behavior_type,
        dim_behavior.is_tdoe_zero_tolerance,
        dim_behavior.k_behavior
    from stg_stu_discipline_incident_behaviors
    join dim_behavior
        on dim_behavior.behavior_code = stg_stu_discipline_incident_behaviors.behavior_type
        and dim_behavior.school_year = stg_stu_discipline_incident_behaviors.school_year
)
select * from behaviors