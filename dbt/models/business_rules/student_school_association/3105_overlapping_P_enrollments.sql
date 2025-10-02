{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3105 %}

with multiple_primary_enrollments as (
    select ssa.school_year, ssa.student_unique_id
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    where ssa.is_primary_school = true
        /* Remove no show enrollments. */
        and not exists (
            select 1
            from {{ ref('no_show_enrollments') }} ns
            where ns.k_student = ssa.k_student
                and ns.k_school = ssa.k_school
                and ns.k_school_calendar = ssa.k_school_calendar
                and ns.tenant_code = ssa.tenant_code
                and ns.is_primary_school = ssa.is_primary_school
                and ns.entry_date = ssa.entry_date
        )
        {{ school_year_exists(error_code, 'ssa') }}
    group by ssa.school_year, ssa.student_unique_id
    having count(*) > 1
), 
student_enrolled_days as (
    select ssa.*, s.state_student_id, schools.school_short_name,
        cd.calendar_date
    from multiple_primary_enrollments x
    join {{ ref('stg_ef3__student_school_associations') }} ssa
        on ssa.school_year = x.school_year
        and ssa.student_unique_id = x.student_unique_id
        and ssa.is_primary_school = true
        /* Remove no show enrollments. */
        and not exists (
            select 1
            from {{ ref('no_show_enrollments') }} ns
            where ns.k_student = ssa.k_student
                and ns.k_school = ssa.k_school
                and ns.k_school_calendar = ssa.k_school_calendar
                and ns.tenant_code = ssa.tenant_code
                and ns.is_primary_school = ssa.is_primary_school
                and ns.entry_date = ssa.entry_date
        )
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    join {{ ref('stg_ef3__schools') }} schools
        on schools.k_school = ssa.k_school
    join {{ ref('stg_ef3__calendar_dates') }} cd
        on cd.k_school_calendar = ssa.k_school_calendar
        and cd.tenant_code = ssa.tenant_code
    join {{ ref('stg_ef3__calendar_dates__calendar_events') }} cde
        on cde.k_school_calendar = cd.k_school_calendar
        and cde.k_calendar_date = cd.k_calendar_date
        and cde.tenant_code = cd.tenant_code
        and cde.calendar_event = 'ID'
    where cd.calendar_date >= ssa.entry_date
        and cd.calendar_date < coalesce(ssa.exit_withdraw_date, to_date(concat(ssa.school_year, '-07-01')))
),
errors as (
    /* A student cannot have more than one active P enrollment at a time.
        By Instructional Days... sigh. */
    /* Do this by overlaps within the same School first so we can only get one message for that school. */
    select distinct p1.k_student, p1.k_school, p1.k_school_calendar, p1.school_id, p1.student_unique_id, p1.school_year, 
        p1.entry_date, p1.entry_grade_level,p1.entry_type,
        p1.state_student_id as legacy_state_student_id,
        {{ error_code }} as error_code,
        concat('Students cannot have overlapping Primary Enrollments. ',
            'Student ', p1.student_unique_id, ' (', coalesce(p1.state_student_id, '[no value]'),') has overlapping Primary Enrollments at ',
            p1.school_short_name, ' (SchoolId:', p1.school_id, ') (', date_format(p1.entry_date, 'MM/dd/yyyy'), 
                ' - ', ifnull(date_format(p1.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ') '
            'and (', date_format(p2.entry_date, 'MM/dd/yyyy'), 
                ' - ', ifnull(date_format(p2.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ').'
            ) as error
    from student_enrolled_days p1
    join student_enrolled_days p2
        on p2.school_year = p1.school_year
        and p2.student_unique_id = p1.student_unique_id
        and p2.calendar_date = p1.calendar_date
        and p2.school_id = p1.school_id
        and p1.entry_date < p2.entry_date
        /* This excludes same rows. */
        and not(
            p1.k_student = p2.k_student
            and p1.k_school = p2.k_school
            and p1.k_school_calendar = p2.k_school_calendar
            and p1.entry_date = p2.entry_date
        )
    union all
    /* Now do it when schools aren't equal. */
    select distinct p1.k_student, p1.k_school, p1.k_school_calendar, p1.school_id, p1.student_unique_id, p1.school_year, 
        p1.entry_date, p1.entry_grade_level,p1.entry_type,
        p1.state_student_id as legacy_state_student_id,
        {{ error_code }} as tdoe_severity_code,
        concat('Students cannot have overlapping Primary Enrollments. ',
            'Student ', p1.student_unique_id, ' (', coalesce(p1.state_student_id, '[no value]'),') has overlapping Primary Enrollments at ',
            p1.school_short_name, ' (SchoolId:', p1.school_id, ') (', date_format(p1.entry_date, 'MM/dd/yyyy'), 
                ' - ', ifnull(date_format(p1.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ') '
            'and ', p2.school_short_name, ' (SchoolId:', p2.school_id, ') (', date_format(p2.entry_date, 'MM/dd/yyyy'), 
                ' - ', ifnull(date_format(p2.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ').'
            ) as error
    from student_enrolled_days p1
    join student_enrolled_days p2
        on p2.school_year = p1.school_year
        and p2.student_unique_id = p1.student_unique_id
        and p2.calendar_date = p1.calendar_date
        and p2.school_id != p1.school_id
    order by p1.school_year, p1.student_unique_id, p1.entry_date
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}