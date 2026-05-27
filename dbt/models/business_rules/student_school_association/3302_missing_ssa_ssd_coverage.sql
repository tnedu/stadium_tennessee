{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3302 %}

/* A Student's SSD must cover their entire enrollment period. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity,
        rule_model
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
    and rule_model = '{{this.identifier}}'
),
ssas as (
    select 
        ssa.k_student, ssa.k_school, ssa.k_school_calendar, cast(ssa.school_id as int) as school_id,
        ssa.student_unique_id, cast(ssa.school_year as int) as school_year, ssa.entry_date, 
        ssa.exit_withdraw_date, ssa.entry_grade_level, ssa.calendar_code
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join brule brule
        on cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    /* Valid enrollments only. We have to edit this once the zero-day early grads goes to prod. */
    where exists (
        select 1
        from {{ ref('valid_enrollments') }} ve
        where ve.k_student = ssa.k_student
            and ve.k_school = ssa.k_school
            and ve.k_school_calendar = ssa.k_school_calendar
            /* to add when zero-day early grads goes to prod. */
            /*and ve.is_zeroday_early_graduate = 0 */
        )
),
ssa_ssd as (
    select 
        ssas.*,
        sd.col.effectiveDate::date as ssd_date_start
    from ssas
    lateral view outer explode(studentStandardDays) sd
),
first_ssd_per_student as (
    select k_student, k_school, cast(school_year as int) as school_year, 
        min(ssd_date_start) as ssd_date_start
    from ssa_ssd 
    group by k_student, k_school, cast(school_year as int)
),
calendar_dates as (
    select cd.k_calendar_date, cd.k_school_calendar, c.k_school, cd.tenant_code,
        cd.school_year, cd.calendar_date, summarize_calendar_events.is_school_day
    from {{ ref('stg_ef3__calendar_dates') }} cd
    join {{ ref('stg_ef3__calendars') }} c
        on cd.k_school_calendar = c.k_school_calendar
    join (
        select 
            ce.k_calendar_date,
            -- if there are multiple events on a day, having at least one 
            -- that counts as a school day applies to the whole day
            sum(xce.is_school_day::integer) >= 1 as is_school_day
        from {{ ref('stg_ef3__calendar_dates__calendar_events') }} ce
        join {{ ref('xwalk_calendar_events') }} xce
            on ce.calendar_event = xce.calendar_event_descriptor
        group by 1
        ) summarize_calendar_events
        on cd.k_calendar_date = summarize_calendar_events.k_calendar_date
    where summarize_calendar_events.is_school_day = true
),
enrollments_and_ssd_date as (
    select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id,  
        ssa.student_unique_id, ssa.school_year, ssa.entry_date, ssa.exit_withdraw_date,
        ssa.entry_grade_level, ssa.calendar_code, fssd.ssd_date_start,
        case
            when fssd.ssd_date_start is null then 0
            when fssd.ssd_date_start > ssa.entry_date then 0
            else 1
        end as ssd_good
    from ssa_ssd ssa
    left outer join first_ssd_per_student fssd
        on fssd.k_school = ssa.k_school
        and fssd.k_student = ssa.k_student
        and fssd.school_year = ssa.school_year
    where 
        /* Enrollment dates must include at least one school day. This eliminates no shows. */
        exists (
            select 1
            from calendar_dates cd
            where cd.k_school = ssa.k_school
                and cd.school_year = ssa.school_year
                and cd.calendar_date >= ssa.entry_date
                and (ssa.exit_withdraw_date is null or cd.calendar_date < ssa.exit_withdraw_date)
        )
),
errors as (
    select 
        x.k_student, 
        x.k_school, 
        x.k_school_calendar,
        x.school_id, 
        x.student_unique_id, 
        x.school_year,
        x.entry_date,
        x.entry_grade_level,
        x.calendar_code,
        s.state_student_id as legacy_state_student_id,
        brule.tdoe_error_code as error_code,
        concat('Student Standard Day missing for Student: ', x.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]') ,'), ',
            'District: ', {{ get_district_from_school_id('x.school_id') }}, ', ',
            'School: ', x.school_id, ', ',
            'Enrollment Entry Date: ', x.entry_date, ', ',
            'Enrollment End Date: ', coalesce(x.exit_withdraw_date, '[null]'), ', ',
            'First SSD Date: ', coalesce(x.ssd_date_start, '[null]'), '.') as error
    from enrollments_and_ssd_date x
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = x.k_student
    join brule
        on x.school_year between brule.error_school_year_start and brule.error_school_year_end
    where x.ssd_good = 0
)
select errors.*,
    {{ severity_to_severity_code_case_clause('brule.tdoe_severity') }},
    brule.tdoe_severity
from errors errors
join brule
    on errors.school_year between brule.error_school_year_start and brule.error_school_year_end