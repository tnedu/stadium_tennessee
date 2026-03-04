{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}


select expelled.school_year, expelled.k_student, expelled.k_school, enrolled.is_primary_school,
    'funding' as reason_type,
    1 as reason_count,
    concat('Student is expelled for ', expelled.discipline_action_length,' days (', 
        expelled.discipline_date_begin, ' - ', expelled.discipline_date_end, '). Enrollment Entry Date: ', enrolled.entry_date, '.') as possible_reason
from {{ ref('wrk_expulsion_windows') }} expelled
join {{ ref('valid_enrollments') }} enrolled
    on enrolled.k_student = expelled.k_student
    and enrolled.k_school = expelled.k_school
    and enrolled.school_year = expelled.school_year