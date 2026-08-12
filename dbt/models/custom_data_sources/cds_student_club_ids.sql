with cohorts as (
    select k_cohort, k_school, cohort_id
    from {{ ref('dim_cohort') }}
    where k_school is not null and k_lea is null
),
school_edorg_ids_cleaned as (
    select ids.k_student, cohort.k_cohort, cohort.cohort_id,
        max(case when id_system = 'CTEChapterid' then id_code end) as CTEChapterId,
        max(case when id_system = 'CTEMembershipid' then id_code end) as CTEMembershipId
    from {{ ref('stg_ef3__stu_ed_org__identification_codes') }} ids
    join cohorts cohort
        on cohort.k_school = ids.k_school
        and upper(ids.assigning_org) LIKE '%' || upper(cohort.cohort_id) || '%'
    where ids.k_school is not null
    group by ids.k_student, cohort.k_cohort, cohort.cohort_id
),
formatted as (
    select fsca.k_student, fsca.k_cohort, fsca.cohort_begin_date as cohort_begindate, 
        CTE_cols.CTEChapterId, CTE_cols.CTEMembershipId
    from {{ ref('stg_ef3__student_cohort_associations') }} fsca
    join school_edorg_ids_cleaned CTE_cols
        on CTE_cols.k_student = fsca.k_student
        and CTE_cols.k_cohort = fsca.k_cohort
)
select *
from formatted