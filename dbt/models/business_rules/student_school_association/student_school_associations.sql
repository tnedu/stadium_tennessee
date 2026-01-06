{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3101_entry_date') }}
union
select *
from {{ ref('3102_exit_withdrawal_date') }}
union
select *
from {{ ref('3103_entry_exit_withdrawal_date') }}
union
select *
from {{ ref('3104_S_enrollment_w_no_P_enrollment') }}
union
select *
from {{ ref('3105_overlapping_P_enrollments') }}
union
select *
from {{ ref('3106_multiple_E_E1_enrollments') }}
union
select *
from {{ ref('3107_TR_enrollment_w_no_E_E1') }}
union
select *
from {{ ref('3108_too_many_entry_types') }}