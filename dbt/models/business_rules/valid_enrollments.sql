{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

with school_day_events as (
    select *
    from {{ ref('xwalk_calendar_events') }}
    where is_school_day = true
),
instructional_days as (
    select cd.k_school_calendar, cd.tenant_code, cd.calendar_date
    from {{ ref('stg_ef3__calendar_dates') }} cd
    join {{ ref('stg_ef3__calendar_dates__calendar_events') }} cde
        on cde.k_school_calendar = cd.k_school_calendar
        and cde.k_calendar_date = cd.k_calendar_date
        and cde.tenant_code = cd.tenant_code
        and cde.calendar_event in (select calendar_event_descriptor from school_day_events)
)
/* These are normal good enrollments because there exist school days within the enrollment window. */
select ssa.k_student, school.k_lea, school.lea_id, ssa.k_school, ssa.k_school_calendar,
    ssa.tenant_code, ssa.api_year, ssa.school_id, ssa.student_unique_id,
    ssa.school_year, ssa.is_primary_school, ssa.entry_date, ssa.exit_withdraw_date,
    ssa.calendar_code, ssa.entry_type, ssa.exit_withdraw_type,
    case
        when ssa.entry_date = ssa.exit_withdraw_date and ssa.exit_withdraw_type = '12: Early Graduate' then 1
        else 0
    end as is_zeroday_early_graduate
from {{ ref('stg_ef3__student_school_associations') }} ssa
join {{ ref('stg_ef3__schools')}} school
    on school.k_school = ssa.k_school
where 
    exists (
        select 1
        from instructional_days cal
        where cal.k_school_calendar = ssa.k_school_calendar
            and cal.tenant_code = ssa.tenant_code
            and cal.calendar_date >= ssa.entry_date 
            and cal.calendar_date < coalesce(ssa.exit_withdraw_date, to_date(concat(ssa.school_year, '-07-01')))
    )
    /* But we also need to include zero-day early grads. 
       This criteria is what defines a zero-day early grad. */
    or (ssa.entry_date = ssa.exit_withdraw_date
        and ssa.exit_withdraw_type = '12: Early Graduate')