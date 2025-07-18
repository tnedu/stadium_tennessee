{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3105 %}

with multiple_primary_enrollments as (
    select ssa.school_year, ssa.student_unique_id
    from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
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
    select ssa.*, cd.calendar_date
    from multiple_primary_enrollments x
    join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
        on ssa.school_year = x.school_year
        and ssa.student_unique_id = x.student_unique_id
    join {{ ref('stg_ef3__calendar_dates_orig') }} cd
        on cd.k_school_calendar = ssa.k_school_calendar
        and cd.tenant_code = ssa.tenant_code
    join {{ ref('stg_ef3__calendar_dates__calendar_events_orig') }} cde
        on cde.k_school_calendar = cd.k_school_calendar
        and cde.k_calendar_date = cd.k_calendar_date
        and cde.tenant_code = cd.tenant_code
        and cde.calendar_event = 'ID'
    where cd.calendar_date >= ssa.entry_date
        and cd.calendar_date < coalesce(ssa.exit_withdraw_date, to_date(concat(ssa.school_year, '-07-01')))
)
/* A student cannot have more than one active P enrollment at a time.
    By Instructional Days... sigh. */
/* Do this by overlaps within the same School first so we can only get one message for that school. */
select distinct p1.k_student, p1.k_school, p1.k_school_calendar, p1.school_id, p1.student_unique_id, p1.school_year, 
    p1.entry_date, p1.entry_grade_level,
    {{ error_code }} as error_code,
    concat('Students cannot have overlapping Primary Enrollments. ',
        'Student "', p1.student_unique_id, '" has overlapping Primary Enrollments at ',
        'School ', p1.school_id, ' (', date_format(p1.entry_date, 'MM/dd/yyyy'), 
            ' - ', ifnull(date_format(p1.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ') '
        'and (', date_format(p2.entry_date, 'MM/dd/yyyy'), 
            ' - ', ifnull(date_format(p2.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ').'
        ) as error,
    {{ error_severity_column(error_code, 'p1') }}
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
    p1.entry_date, p1.entry_grade_level,
    {{ error_code }} as error_code,
    concat('Students cannot have overlapping Primary Enrollments. ',
        'Student "', p1.student_unique_id, '" has overlapping Primary Enrollments at ',
        'School ', p1.school_id, ' (', date_format(p1.entry_date, 'MM/dd/yyyy'), 
            ' - ', ifnull(date_format(p1.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ') '
        'and School ', p2.school_id, ' (', date_format(p2.entry_date, 'MM/dd/yyyy'), 
            ' - ', ifnull(date_format(p2.exit_withdraw_date, 'MM/dd/yyyy'), 'null'), ').'
        ) as error,
    {{ error_severity_column(error_code, 'p1') }}
from student_enrolled_days p1
join student_enrolled_days p2
    on p2.school_year = p1.school_year
    and p2.student_unique_id = p1.student_unique_id
    and p2.calendar_date = p1.calendar_date
    and p2.school_id != p1.school_id
order by p1.school_year, p1.student_unique_id, p1.entry_date