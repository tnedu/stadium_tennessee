{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}


select c.*, cd.k_calendar_date, cd.calendar_date, cd.n_calendar_events, ce.calendar_event, 
    xwalk.pct_of_day, xwalk.is_school_day, xwalk.is_statutory_day, xwalk.is_discretionary_day,
    xwalk.is_parent_teacher_day, xwalk.is_abbreviated_day, xwalk.is_inservice_day, 
    xwalk.is_stockpile_day, xwalk.is_teacher_vacation_day
from {{ ref('stg_ef3__calendars') }} c
left outer join {{ ref('stg_ef3__calendar_dates') }} cd
    on cd.k_school_calendar = c.k_school_calendar
left outer join {{ ref('stg_ef3__calendar_dates__calendar_events') }} ce
    on ce.k_school_calendar = cd.k_school_calendar
    and ce.k_calendar_date = cd.k_calendar_date
left outer join {{ ref('xwalk_calendar_events') }} xwalk
    on xwalk.calendar_event_descriptor = ce.calendar_event
