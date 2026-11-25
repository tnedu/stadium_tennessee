{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stage_class_periods as (
    select * from {{ ref('stg_ef3__class_periods') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_class_period,
        {{ json_flatten_index('meeting_time') }} + 1 as meeting_time_index,
        value:startTime::string as start_time,
        value:endTime::string as end_time,
        case
            when start_time > end_time then true
            else false
        end as has_end_time_before_start_time,
        -- Create military time columns for the time math
        {{ to_military_time('start_time') }} as military_start_time,
        {{ to_military_time('end_time') }} as military_end_time,
        timediff(MINUTE, concat('2020-01-01 ', military_start_time)::timestamp, concat('2020-01-01 ', military_end_time)::timestamp) as meeting_time_duration
    from stage_class_periods
    /* This purposefully has all rows flattened, even nulls. */
        {{ edu_edfi_source.json_flatten('v_meeting_times') }}
)
select * from flattened
