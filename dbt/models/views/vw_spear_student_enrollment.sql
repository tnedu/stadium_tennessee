{{
  config(
    materialized="table",
    schema="spear"
  )
}}

with q as (
select distinct ssa.school_year, lea.lea_id, lea.lea_name , regexp_replace(right(sc.school_id, 4), '^0+', '') as school_number,
sc.school_id ,
                sc.school_name, s.student_unique_id, s.state_student_id,
       s.last_name, s.first_name, ssa.grade_level_integer,
       ssa.entry_date, ssa.exit_withdraw_date,
        row_number() over (
            partition by ssa.SCHOOL_YEAR, student_unique_id
            order by ssa.entry_date desc,
                coalesce(ssa.exit_withdraw_date, to_date('9999-12-31')) desc
        ) as rnk
from teds_prod.edfi_wh.fct_student_school_association ssa
         join teds_prod.edfi_wh.dim_student s on ssa.k_student = s.k_student
         join teds_prod.edfi_wh.dim_school sc on ssa.k_school = sc.k_school
        join teds_prod.edfi_wh.dim_lea lea on sc.k_lea = lea.k_lea
         where ssa.school_year = 2026 and
               ssa.is_primary_school = 'True' and
               --ssa.entry_grade_level not in ('P3','P4') and
        (exit_withdraw_date is null or entry_date < exit_withdraw_date))
select school_year, student_unique_id, first_name, last_name, grade_level_integer, school_number, school_name, lea_id, lea_name
from q
where rnk = 1
and exit_withdraw_date is null
order by lea_id, school_number;