{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with all as (
    select dcs.k_course_section, dcp.k_class_period, bscp.k_bell_schedule, bsd.calendar_date,
      max(greatest(dcs.tdoe_severity_code, dcp.tdoe_severity_code, coalesce(bs.tdoe_severity_code,0))) as tdoe_severity_code
  from {{ ref('dim_class_period') }} dcp 
  join teds_dev.dev_smckee_stage.stg_ef3__sections__class_periods scp
      on scp.k_class_period = dcp.k_class_period
  join {{ ref('dim_course_section') }} dcs
      on dcs.k_course_section = scp.k_course_section
  join {{ ref('stg_ef3__bell_schedules__class_periods') }} bscp
      on bscp.k_class_period = dcp.k_class_period
  join {{ ref('stg_ef3__bell_schedules__dates') }} bsd
      on bsd.k_bell_schedule = bscp.k_bell_schedule
  left outer join {{ ref('bell_schedules') }} bs
      on bs.k_bell_schedule = bsd.k_bell_schedule
  group by dcs.k_course_section, dcp.k_class_period, bscp.k_bell_schedule, bsd.calendar_date
  having max(greatest(dcs.tdoe_severity_code, dcp.tdoe_severity_code, coalesce(bs.tdoe_severity_code,0))) > 0
)
select k_course_section, k_class_period, k_bell_schedule, calendar_date,
    tdoe_severity_code,
    {{ severity_code_to_severity_case_clause('tdoe_severity_code')}}
from all