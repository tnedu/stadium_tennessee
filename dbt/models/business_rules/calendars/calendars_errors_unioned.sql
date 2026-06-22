{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

select *
from {{ ref('1000_not_enough_instructional_days') }}
union
select *
from {{ ref('1001_calendar_coexistence_issues') }}
union
select *
from {{ ref('1003_calendar_date_not_within_schoolyear') }}
union
select *
from {{ ref('1004_required_calendar_events') }}
union
select *
from {{ ref('1005_inservice_day_count') }}
union
select *
from {{ ref('1006_stockpile_day_count') }}
union
select *
from {{ ref('1007_parent_teacher_day_count') }}
union
select *
from {{ ref('1008_discretionary_day_count') }}
union
select *
from {{ ref('1009_school_day_not_on_bell_schedule') }}
union
select *
from {{ ref('1010_teacher_vacation_day_count') }}