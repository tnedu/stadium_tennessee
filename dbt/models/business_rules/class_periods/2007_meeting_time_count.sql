{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2007 %}

with stg_class_periods as (
    select * from {{ ref('stg_ef3__class_periods') }} cp
    where 1=1
        {{ school_year_exists(error_code, 'cp') }}
),
tooManyClassPeriods as (
    select cp.k_class_period,
        v_meeting_times,
        size(cast(v_meeting_times as array<string>)) meetingTimesCount
    from stg_class_periods cp
    where size(cast(v_meeting_times as array<string>)) > 1
),
errors as (
    /* Class Periods cannot have more than one meeting time. */
    select cp.k_class_period, cast(cp.school_year as int) as school_year, cp.class_period_name, cp.school_id,
        {{ error_code }} as error_code,
        concat('Class Period ', cp.class_period_name, ' has more than one meeting time. Meeting Times: ', cast(cp.v_meeting_times as String)) as error
    from stg_class_periods cp
    join tooManyClassPeriods x
        on x.k_class_period = cp.k_class_period
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}
