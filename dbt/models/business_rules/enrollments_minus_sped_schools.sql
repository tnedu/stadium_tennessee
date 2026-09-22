 select ssa.k_student, school.k_lea, school.lea_id, ssa.k_school, ssa.school_id, ssa.k_school_calendar,
        ssa.tenant_code, ssa.api_year,  ssa.student_unique_id,ssa.school_year, ssa.is_primary_school,
        ssa.entry_date, ssa.exit_withdraw_date, ssa.calendar_code
    from {{ ref('stg_ef3__student_school_associations') }} ssa
    join {{ ref('stg_ef3__schools')}} school
    on school.k_school = ssa.k_school
    where (cast(right(right(concat('00000000', ssa.school_id), 8), 4) as int)  <= 4800 
                or cast(right(right(concat('00000000', ssa.school_id), 8), 4) as int) >= 4999)