{{
  config(
    materialized = "table",
    schema = "stg_data_errors"
  )
}}

{% set error_code = 3404 %}

with brule as (

    select
        tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }}
    where tdoe_error_code = {{ error_code }}
      and rule_model = '{{ this.identifier }}'

),

ssas as (

    select
        ssa.k_student,
        ssa.k_school,
        ssa.k_school_calendar,
        cast(ssa.school_id as int) as school_id,
        ssa.student_unique_id,
        cast(ssa.school_year as int) as school_year,
        ssa.entry_date,
        ssa.exit_withdraw_date,
        ssa.studentStandardDays
    from {{ ref('stg_ef3__student_school_associations') }} ssa

    join brule
        on cast(ssa.school_year as int)
            between brule.error_school_year_start
                and brule.error_school_year_end

    where exists (
        select 1
        from {{ ref('valid_enrollments') }} ve
        where ve.k_student = ssa.k_student
          and ve.k_school = ssa.k_school
          and ve.k_school_calendar = ssa.k_school_calendar
    )

),

ssa_ssd as (

    select
        ssa.k_student,
        ssa.k_school,
        ssa.k_school_calendar,
        ssa.school_id,
        ssa.student_unique_id,
        ssa.school_year,
        ssa.entry_date,
        ssa.exit_withdraw_date,

        sd.col.effectiveDate::date as effective_date,
        sd.col.studentStandardDayDuration::int as ssd_duration

    from ssas ssa

    lateral view explode(ssa.studentStandardDays) sd

),

ssa_ssd_ranges as (

    select
        k_student,
        k_school,
        k_school_calendar,
        school_id,
        student_unique_id,
        school_year,
        entry_date,
        exit_withdraw_date,
        effective_date,

        lead(effective_date) over (
            partition by
                k_student,
                k_school,
                k_school_calendar,
                school_id,
                school_year
            order by effective_date
        ) as next_effective_date,

        ssd_duration

    from ssa_ssd

),

student_ssd as (

    select
        ssd.k_student,
        ssd.k_school,
        ssd.k_school_calendar,
        ssd.school_id,
        ssd.student_unique_id,
        ssd.school_year,
        ssd.entry_date,
        rp.report_period,

        sum(ssd.ssd_duration) as sum_ssd_mins

    from ssa_ssd_ranges ssd

    join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = ssd.k_school_calendar
       and cd.calendar_date >= greatest(
            ssd.effective_date,
            ssd.entry_date
       )
       and (
            ssd.next_effective_date is null
            or cd.calendar_date < ssd.next_effective_date
       )
       and (
            ssd.exit_withdraw_date is null
            or cd.calendar_date <= ssd.exit_withdraw_date
       )

    join {{ ref('cds_calendar_date_report_periods') }} rp
        on rp.k_calendar_date = cd.k_calendar_date

    group by
        ssd.k_student,
        ssd.k_school,
        ssd.k_school_calendar,
        ssd.school_id,
        ssd.student_unique_id,
        ssd.school_year,
        ssd.entry_date,
        rp.report_period

),

student_schedule as (

    select
        ssea.k_student,
        ssa.k_school,
        ssa.k_school_calendar,
        cast(ssea.school_id as int) as school_id,
        ssea.student_unique_id,
        cast(ssea.school_year as int) as school_year,
        ssa.entry_date,
        rp.report_period,

        sum(csd.period_duration) as sum_class_duration

    from {{ ref('stg_ef3__student_section_associations') }} ssea

    join {{ ref('stg_ef3__student_school_associations') }} ssa
        on ssa.k_student = ssea.k_student
       and ssa.school_year = ssea.school_year
       and ssa.school_id = ssea.school_id

    join {{ ref('course_section_days') }} csd
        on csd.k_course_section = ssea.k_course_section
       and csd.school_year = ssea.school_year
       and csd.calendar_date between ssea.begin_date and ssea.end_date
       and csd.calendar_date >= ssa.entry_date
       and (
            ssa.exit_withdraw_date is null
            or csd.calendar_date <= ssa.exit_withdraw_date
       )

    join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = ssa.k_school_calendar
       and cd.calendar_date = csd.calendar_date

    join {{ ref('cds_calendar_date_report_periods') }} rp
        on rp.k_calendar_date = cd.k_calendar_date

    join brule
        on cast(ssea.school_year as int)
            between brule.error_school_year_start
                and brule.error_school_year_end

    group by
        ssea.k_student,
        ssa.k_school,
        ssa.k_school_calendar,
        cast(ssea.school_id as int),
        ssea.student_unique_id,
        cast(ssea.school_year as int),
        ssa.entry_date,
        rp.report_period

),

under_scheduled as (

    select
        sch.k_student,
        sch.k_school,
        sch.k_school_calendar,
        sch.school_id,
        sch.student_unique_id,
        sch.school_year,
        sch.entry_date,
        sch.report_period,
        sch.sum_class_duration,
        ssd.sum_ssd_mins

    from student_schedule sch

    join student_ssd ssd
        on ssd.k_student = sch.k_student
       and ssd.k_school = sch.k_school
       and ssd.k_school_calendar = sch.k_school_calendar
       and ssd.school_id = sch.school_id
       and ssd.school_year = sch.school_year
       and ssd.entry_date = sch.entry_date
       and ssd.report_period = sch.report_period

    where sch.sum_class_duration < ssd.sum_ssd_mins

),

bad_records as (

    select
        us.k_student,
        ssea.k_course_section,
        us.school_id,
        us.student_unique_id,
        ssea.local_course_code,
        us.school_year,
        ssea.section_id,
        ssea.begin_date,

        us.entry_date,
        us.report_period,
        us.sum_class_duration,
        us.sum_ssd_mins,

        brule.tdoe_error_code as error_code,
        brule.tdoe_severity,

        row_number() over (
            partition by
                us.k_student,
                us.school_id,
                us.school_year,
                us.report_period
            order by
                ssea.begin_date,
                ssea.k_course_section
        ) as rn

    from under_scheduled us

    join {{ ref('stg_ef3__student_section_associations') }} ssea
        on ssea.k_student = us.k_student
       and ssea.school_id = us.school_id
       and ssea.school_year = us.school_year

    join brule
        on us.school_year
            between brule.error_school_year_start
                and brule.error_school_year_end

),

errors as (

    select
        bad.k_student,
        bad.k_course_section,
        bad.school_id,
        bad.student_unique_id,
        bad.local_course_code,
        bad.school_year,
        bad.section_id,
        bad.begin_date,

        s.state_student_id as legacy_state_student_id,

        bad.error_code,

        concat(
            'Student is under scheduled - Student Unique ID: ',
            coalesce(bad.student_unique_id, '[null]'),
            ', School ID: ',
            coalesce(cast(bad.school_id as string), '[null]'),
            ', Enrollment Date: ',
            coalesce(cast(bad.entry_date as string), '[null]'),
            ', Reporting Period: ',
            coalesce(cast(bad.report_period as string), '[null]'),
            ', Sum of class duration: ',
            coalesce(cast(bad.sum_class_duration as string), '0'),
            ', Sum of SSD mins: ',
            coalesce(cast(bad.sum_ssd_mins as string), '0'),
            '.'
        ) as error,

        {{ severity_to_severity_code_case_clause('bad.tdoe_severity') }},
        bad.tdoe_severity

    from bad_records bad

    join {{ ref('stg_ef3__students') }} s
        on s.k_student = bad.k_student

    where bad.rn = 1

)

select *
from errors
