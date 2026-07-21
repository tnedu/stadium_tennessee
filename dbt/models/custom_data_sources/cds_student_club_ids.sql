{{
  config(
    materialized="table",
    schema="cds"
  )
}}
select k_student, k_student_xyear, k_school, assigning_org,
       max(case when id_system = 'CTEChapterid' then id_code end) as CTEChapterid,
       max(case when id_system = 'CTEMembershipid' then id_code end) as CTEMembershipid
from {{ ref('stg_ef3__stu_ed_org__identification_codes') }}
where k_school is not null
and id_system in ('CTEChapterid','CTEMembershipid')
group by k_student, k_student_xyear, k_school, assigning_org