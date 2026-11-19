{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get every student by day and all their various flags for that day.
This is needed to determine the membership calculation, so the main column here is the "isAMember" column.
*/

with student_calendar_dates as (
    /*
     * Establish one row per valid enrollment and applicable school day.
     *
     * Normal enrollments stop producing rows at exit.
     * Early graduates continue producing rows after exit because they
     * continue receiving ADM.
     *
     * Grade-based FTE is resolved here because the applicable grade is
     * already known at this student-enrollment-day grain.
     */
    select
        fssa.k_student,
        fssa.k_lea,
        fssa.k_school,
        fssa.k_school_calendar,
        fssa.tenant_code,
        fssa.school_year,
        fssa.is_primary_school,
        fssa.entry_date,
        fssa.exit_withdraw_date,
        fssa.exit_withdraw_type,

        gl.grade_level_short as grade_level,
        gl.grade_level_adm,

        fte_grade.weight as fte_grade_weight,
        fte_grade.statutory_program as fte_grade_statutory_program,

        dcd.calendar_date,
        dcd.day_of_school_year,
        dcd.report_period,
        dcd.report_period_begin_date,
        dcd.report_period_end_date,
        dcd.school_days_in_report_period,

        ve.is_zeroday_early_graduate,

        fssa.tdoe_severity_code as enrollment_severity_code,
        dcd.tdoe_severity_code as calendar_date_severity_code

    from {{ ref('fct_student_school_association') }} fssa

    join {{ ref('valid_enrollments') }} ve
        on ve.k_student = fssa.k_student
        and ve.k_school = fssa.k_school
        and ve.k_school_calendar = fssa.k_school_calendar
        and ve.tenant_code = fssa.tenant_code
        and ve.school_year = fssa.school_year
        and ve.is_primary_school = fssa.is_primary_school
        and ve.entry_date = fssa.entry_date
        and ve.exit_withdraw_date <=> fssa.exit_withdraw_date

    join {{ ref('xwalk_grade_levels') }} gl
        on upper(gl.grade_level) = upper(fssa.entry_grade_level)

    /*
     * A grade can match at most one active Grade FTE/ADA rule for the
     * applicable school year.
     */
    left join {{ ref('fteada_statutory_programs') }} fte_grade
        on fte_grade.program_grouping = 'Grade'
        and fte_grade.program_criterion = gl.grade_level_adm
        and fssa.school_year between
            fte_grade.start_school_year
            and coalesce(fte_grade.end_school_year, 9999)

    join {{ ref('dim_calendar_date') }} dcd
        on dcd.k_school_calendar = fssa.k_school_calendar
        and dcd.is_school_day = true
        and dcd.calendar_date >= fssa.entry_date
        and (
            fssa.exit_withdraw_date is null
            or dcd.calendar_date < fssa.exit_withdraw_date
            or fssa.exit_withdraw_type = '12: Early Graduate'
        )
),

normal_enrollments as (
    select
        scd.k_student,
        scd.k_lea,
        scd.k_school,
        scd.k_school_calendar,
        scd.school_year,
        scd.is_primary_school,
        scd.entry_date,
        scd.exit_withdraw_date,
        scd.grade_level,
        scd.grade_level_adm,

        scd.fte_grade_weight,
        scd.fte_grade_statutory_program,

        /*
         * The SPED candidate exists only when the student is SPED-active on
         * this date. SPED FTE remains independent of Grade and CTE FTE.
         */
        fte_sped.weight as fte_sped_weight,
        fte_sped.statutory_program as fte_sped_statutory_program,

        case
            when scd.exit_withdraw_type = '12: Early Graduate' then 1
            else 0
        end as is_early_graduate,

        0 as is_zeroday_early_graduate,

        ssd.ssd_duration,

        scd.calendar_date,
        scd.day_of_school_year,
        scd.report_period,
        scd.report_period_begin_date,
        scd.report_period_end_date,
        scd.school_days_in_report_period,

        case
            when sped.participation_status is not null then 1
            else 0
        end as is_sped,

        case
            when funding_ineligible.begin_date is not null then 1
            else 0
        end as is_funding_ineligible,

        case
            when expulsions.discipline_date_begin is not null then 1
            else 0
        end as is_expelled,

        case
            when suspensions.discipline_date_begin is not null then 1
            else 0
        end as is_suspended,

        case
            when econdis.begin_date is not null then 1
            else 0
        end as is_econdis,

        case
            when ilp.participation_status is not null then 1
            else 0
        end as is_el,

        case
            when ilpd.service_begin_date is not null then 1
            else 0
        end as is_dyslexic,

        coalesce(attendance.is_absent, 0.0) as is_absent,

        greatest(
            coalesce(scd.enrollment_severity_code, 0),
            coalesce(scd.calendar_date_severity_code, 0),
            coalesce(ssd.tdoe_severity_code, 0),
            coalesce(attendance.tdoe_severity_code, 0)
        ) as tdoe_severity_code

    from student_calendar_dates scd

    left join {{ ref('fct_student_standard_day') }} ssd
        on ssd.k_school = scd.k_school
        and ssd.k_student = scd.k_student
        and scd.calendar_date between
            ssd.ssd_date_start and ssd.ssd_date_end

    left join {{ ref('bld_sped_safe_ranges') }} sped
        on sped.k_lea = scd.k_lea
        and sped.k_student = scd.k_student
        and sped.tenant_code = scd.tenant_code
        and sped.school_year = scd.school_year
        and sped.primary_indicator = true
        and scd.calendar_date between
            sped.service_begin_date and sped.service_end_date

    /*
     * Resolve SPED FTE only after the date-specific SPED range has matched.
     *
     * Unlike Grade and CTE, the SPED rule does not need a criterion match.
     * It applies whenever the student is SPED-active for the date.
     */
    left join {{ ref('fteada_statutory_programs') }} fte_sped
        on fte_sped.program_grouping = 'Sped'
        and sped.participation_status is not null
        and scd.school_year between
            fte_sped.start_school_year
            and coalesce(fte_sped.end_school_year, 9999)

    left join {{ ref('bld_funding_ineligible_safe_ranges') }} funding_ineligible
        on funding_ineligible.k_lea = scd.k_lea
        and funding_ineligible.k_student = scd.k_student
        and funding_ineligible.tenant_code = scd.tenant_code
        and funding_ineligible.school_year = scd.school_year
        and scd.calendar_date between
            funding_ineligible.begin_date
            and funding_ineligible.end_date

    left join {{ ref('bld_expulsion_safe_ranges') }} expulsions
        on expulsions.k_school = scd.k_school
        and expulsions.k_student = scd.k_student
        and expulsions.k_school_calendar = scd.k_school_calendar
        and expulsions.tenant_code = scd.tenant_code
        and expulsions.school_year = scd.school_year
        and expulsions.discipline_action = 'E'
        and scd.calendar_date between
            expulsions.discipline_date_begin
            and expulsions.discipline_date_end

    left join {{ ref('bld_suspension_safe_ranges') }} suspensions
        on suspensions.k_school = scd.k_school
        and suspensions.k_student = scd.k_student
        and suspensions.k_school_calendar = scd.k_school_calendar
        and suspensions.tenant_code = scd.tenant_code
        and suspensions.school_year = scd.school_year
        and suspensions.discipline_action = 'S'
        and scd.calendar_date between
            suspensions.discipline_date_begin
            and suspensions.discipline_date_end

    left join {{ ref('bld_econdis_safe_ranges') }} econdis
        on econdis.k_lea = scd.k_lea
        and econdis.k_student = scd.k_student
        and econdis.tenant_code = scd.tenant_code
        and econdis.school_year = scd.school_year
        and scd.calendar_date between
            econdis.begin_date and econdis.end_date

    left join {{ ref('bld_ilp_safe_ranges') }} ilp
        on ilp.k_school = scd.k_school
        and ilp.k_student = scd.k_student
        and ilp.school_year = scd.school_year

        /*
         * For the first ILP status, extend the active range back to the
         * enrollment date when ILP begins within 60 days of enrollment.
         */
        and scd.calendar_date between
            case
                when ilp.seq = 1
                    and datediff(
                        ilp.status_begin_date,
                        scd.entry_date
                    ) between 0 and 60
                    then scd.entry_date
                else ilp.status_begin_date
            end
            and ilp.status_end_date

    /* Determine the ILPD source location applicable to the school year. */
    left join {{ ref('ilpd_schoolyear_sources') }} ilpd_source_years
        on scd.school_year >= ilpd_source_years.source_school_year_start
        and (
            ilpd_source_years.source_school_year_end is null
            or scd.school_year <= ilpd_source_years.source_school_year_end
        )

    left join {{ ref('bld_ilpd_safe_ranges') }} ilpd
        on ilpd.k_school = scd.k_school
        and ilpd.k_student = scd.k_student
        and ilpd.source_location = ilpd_source_years.source_location
        and scd.calendar_date between
            ilpd.service_begin_date and ilpd.service_end_date

    left join {{ ref('fct_student_daily_attendance') }} attendance
        on attendance.k_student = scd.k_student
        and attendance.k_school = scd.k_school
        and attendance.calendar_date = scd.calendar_date

        /*
         * Early graduates have student-day rows after exit, but attendance
         * applies only while the student is actively enrolled.
         */
        and (
            scd.exit_withdraw_date is null
            or attendance.calendar_date < scd.exit_withdraw_date
        )

        and attendance.is_absent > 0.0

    where scd.is_zeroday_early_graduate = 0
),

zeroday_early_graduates as (
    /*
     * Zero-day early graduates have no normal enrollment window, but receive
     * student-day records beginning on their graduation date.
     *
     * Grade FTE still applies. SPED is intentionally not applied because the
     * existing zero-day branch defines is_sped as zero.
     */
    select
        scd.k_student,
        scd.k_lea,
        scd.k_school,
        scd.k_school_calendar,
        scd.school_year,
        scd.is_primary_school,
        scd.entry_date,
        scd.exit_withdraw_date,
        scd.grade_level,
        scd.grade_level_adm,

        scd.fte_grade_weight,
        scd.fte_grade_statutory_program,

        cast(null as double) as fte_sped_weight,
        cast(null as string) as fte_sped_statutory_program,

        1 as is_early_graduate,
        1 as is_zeroday_early_graduate,

        null as ssd_duration,

        scd.calendar_date,
        scd.day_of_school_year,
        scd.report_period,
        scd.report_period_begin_date,
        scd.report_period_end_date,
        scd.school_days_in_report_period,

        0 as is_sped,
        0 as is_funding_ineligible,
        0 as is_expelled,
        0 as is_suspended,

        case
            when econdis.begin_date is not null then 1
            else 0
        end as is_econdis,

        0 as is_el,
        0 as is_dyslexic,
        0.0 as is_absent,

        greatest(
            coalesce(scd.enrollment_severity_code, 0),
            coalesce(scd.calendar_date_severity_code, 0)
        ) as tdoe_severity_code

    from student_calendar_dates scd

    left join {{ ref('bld_econdis_safe_ranges') }} econdis
        on econdis.k_lea = scd.k_lea
        and econdis.k_student = scd.k_student
        and econdis.tenant_code = scd.tenant_code
        and econdis.school_year = scd.school_year
        and scd.calendar_date between
            econdis.begin_date and econdis.end_date

    where scd.is_zeroday_early_graduate = 1
),

student_days as (
    select * from normal_enrollments

    union all

    select * from zeroday_early_graduates
),

student_days_with_severity as (
    select
        *,
        {{ severity_code_to_severity_case_clause(
            'tdoe_severity_code'
        ) }}
    from student_days
)

select
    k_student,
    k_lea,
    k_school,
    k_school_calendar,
    school_year,
    is_primary_school,
    entry_date,
    exit_withdraw_date,
    grade_level,
    grade_level_adm,

    fte_grade_weight,
    fte_grade_statutory_program,
    fte_sped_weight,
    fte_sped_statutory_program,

    is_early_graduate,
    is_zeroday_early_graduate,
    ssd_duration,
    calendar_date,
    day_of_school_year,
    report_period,
    report_period_begin_date,
    report_period_end_date,
    school_days_in_report_period,
    is_sped,
    is_funding_ineligible,
    is_expelled,
    is_suspended,
    is_econdis,
    is_el,
    is_dyslexic,
    is_absent,

    case
        when is_early_graduate = 1
            and calendar_date >= exit_withdraw_date
            then 1
        else 0
    end as is_early_grad_date,

    case
        /* Early graduates receive membership after graduation. */
        when is_early_graduate = 1
            and calendar_date >= exit_withdraw_date
            then 1

        /* Expelled SPED students retain membership. */
        when is_expelled = 1
            and is_sped = 1
            then 1

        /* Other expelled students do not receive membership. */
        when is_expelled = 1 then 0

        /* Funding-ineligible students do not receive membership. */
        when is_funding_ineligible = 1 then 0

        /*
         * Remaining student-day rows are inside a valid enrollment window
         * and therefore receive membership.
         */
        else 1
    end as isa_member,

    tdoe_severity_code,
    tdoe_severity

from student_days_with_severity