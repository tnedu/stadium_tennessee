-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_class_period_meeting_time set not null",
        "alter table {{ this }} add primary key (k_class_period_meeting_time)",
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add constraint fk_{{ this.name }}_class_period foreign key (k_class_period) references {{ ref('dim_class_period') }}",
    ]
  )
}}

/* This detail model purposefully allows null start_times, even though that would normally be part
   of the PK, so that users can safely inner join to this table and all class period references
   remain visible. */
with class_periods as (
    select * from {{ ref('dim_class_period') }}
),
meeting_times as (
    select *,
        row_number() over (
            partition by k_class_period
            order by start_time, end_time
        ) as meeting_time_sequence
    from {{ ref('stg_ef3__class_periods__meeting_times') }}
),
formatted as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'meeting_times.k_class_period',
                'meeting_times.start_time'
            ]
        ) }} as k_class_period_meeting_time,
        meeting_times.k_class_period,
        class_periods.tenant_code,
        class_periods.school_year,
        meeting_times.meeting_time_index,
        meeting_times.meeting_time_sequence,
        meeting_times.start_time,
        meeting_times.end_time,
        meeting_times.has_end_time_before_start_time,
        meeting_times.military_start_time,
        meeting_times.military_end_time,
        meeting_times.meeting_time_duration
    from meeting_times
    join class_periods
        on class_periods.k_class_period = meeting_times.k_class_period
)
select * from formatted