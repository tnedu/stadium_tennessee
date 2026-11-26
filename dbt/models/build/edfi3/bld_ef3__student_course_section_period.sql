{{
  config(
    materialized="table",
    schema="build",
    liquid_clustered_by=["k_course_section"]
  )
}}

with student_school_associations as (

    select
        k_student_school_association,
        k_student,
        k_school,

        tenant_code,
        school_year,

        entry_date,
        exit_withdraw_date,

        {% if var(
            'edu:enroll:exit_withdraw_date_inclusive',
            true
        ) %}

            exit_withdraw_date

        {% else %}

            dateadd(
                day,
                -1,
                exit_withdraw_date
            )

        {% endif %} as enrollment_end_date

    from {{ ref('fct_student_school_association') }}

),

student_section_associations as (

    select
        k_student,
        k_school,
        k_course_section,

        tenant_code,
        school_year,

        begin_date,
        end_date

    from {{ ref('fct_student_section_association') }}

),

student_course_section_periods as (

    /*
     * Grain:
     * One student-school enrollment episode x one
     * StudentSectionAssociation.
     *
     * The effective period is the intersection of:
     *
     *   StudentSchoolAssociation
     *     entry_date / normalized enrollment_end_date
     *
     *   StudentSectionAssociation
     *     begin_date / end_date
     */
    select
        enrollments.k_student_school_association,

        sections.k_student,
        sections.k_school,
        sections.k_course_section,

        sections.tenant_code,
        sections.school_year,

        /*
         * Retained because begin_date is part of the natural key of the
         * StudentSectionAssociation.
         */
        sections.begin_date,

        greatest(
            sections.begin_date,
            enrollments.entry_date
        ) as effective_begin_date,

        case
            when sections.end_date is null
                and enrollments.enrollment_end_date is null
            then null

            when sections.end_date is null
            then enrollments.enrollment_end_date

            when enrollments.enrollment_end_date is null
            then sections.end_date

            else least(
                sections.end_date,
                enrollments.enrollment_end_date
            )
        end as effective_end_date

    from student_section_associations sections

    join student_school_associations enrollments
        on enrollments.k_student =
            sections.k_student

        and enrollments.k_school =
            sections.k_school

        and enrollments.tenant_code =
            sections.tenant_code

        and enrollments.school_year =
            sections.school_year

        /*
         * Require the two intervals to intersect.
         *
         * The enrollment end date has already been normalized to an
         * inclusive last-enrolled date.
         */
        and enrollments.entry_date <= coalesce(
            sections.end_date,
            date('9999-12-31')
        )

        and coalesce(
            enrollments.enrollment_end_date,
            date('9999-12-31')
        ) >= sections.begin_date

),

formatted as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'k_student_school_association',
            'k_student',
            'k_course_section',
            'begin_date'
        ]) }} as k_student_course_section_period,

        k_student_school_association,

        k_student,
        k_school,
        k_course_section,

        tenant_code,
        school_year,

        begin_date,

        effective_begin_date,
        effective_end_date

    from student_course_section_periods

    /*
     * Removes empty effective intervals, including Tennessee no-shows:
     *
     *   entry_date                 = 2026-08-01
     *   exclusive exit date       = 2026-08-01
     *   normalized enrollment end = 2026-07-31
     *
     * The source enrollment remains in fct_student_school_association,
     * but it cannot produce a schedule period.
     */
    where effective_begin_date is not null
        and (
            effective_end_date is null
            or effective_begin_date <= effective_end_date
        )

)

select *
from formatted