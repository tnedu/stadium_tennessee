{{
  config(
    materialized="table",
    schema="cds"
  )
}}
with cohort_ids as (
  select distinct cohort_id, k_school
  from {{ ref('stg_ef3__cohorts') }}
)
select seoic.k_student, seoic.k_student_xyear, seoic.k_school, seoic.assigning_org, cohort_ids.cohort_id,
       max(case when seoic.id_system = 'CTEChapterid' then seoic.id_code end) as CTEChapterid,
       max(case when seoic.id_system = 'CTEMembershipid' then seoic.id_code end) as CTEMembershipid
from {{ ref('stg_ef3__stu_ed_org__identification_codes') }} seoic
join cohort_ids
  on seoic.k_school = cohort_ids.k_school
  and upper(seoic.assigning_org) LIKE '%' || upper(cohort_ids.cohort_id) || '%'
where seoic.k_school is not null
and seoic.id_system in ('CTEChapterid','CTEMembershipid')
group by seoic.k_student, seoic.k_student_xyear, seoic.k_school, seoic.assigning_org, cohort_ids.cohort_id