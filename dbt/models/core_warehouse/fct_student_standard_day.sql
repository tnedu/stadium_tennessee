{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column student_unique_id set not null",
        "alter table {{ this }} alter column ssd_date_start set not null",
        "alter table {{ this }} add primary key (k_student, k_school, student_unique_id, ssd_date_start)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('edu_wh', 'dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('edu_wh', 'dim_school') }}"
    ]
  )
}}

select ssae.k_student, ssae.k_student_xyear, ssae.k_school,
    ssae.k_session,
    ssae.tenant_code,
    ssae.school_year, 
    ssae.student_unique_id, 
    ssae.attendance_event_date as ssd_date_start,
    coalesce(
      date_add(
        lead(ssae.attendance_event_date) over (
          partition by ssae.k_student, ssae.k_school
          order by ssae.attendance_event_date),
        -1)
      , to_date(concat(ssae.school_year, '-06-30'), 'yyyy-MM-dd')) as ssd_date_end, 
    ssae.school_attendance_duration as ssd_duration
from {{ ref('stg_ef3__student_school_attendance_events') }} ssae
where ssae.attendance_event_category = 'SSD'
order by ssae.k_school, ssae.k_student, ssae.attendance_event_date