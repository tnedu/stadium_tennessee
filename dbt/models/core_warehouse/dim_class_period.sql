-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add primary key (k_class_period)",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}
{# Load custom data sources from var #}
{% set custom_data_sources = var("edu:class_period:custom_data_sources", []) %}

with class_periods as (
    select * from {{ ref('stg_ef3__class_periods') }}
),
meeting_times as (
    select * from {{ ref('stg_ef3__class_periods__meeting_times') }}
),
singleton_meeting_times as (
    select * 
    from meeting_times
    qualify count(*) over (
        partition by k_class_period
    ) = 1
),
sum_meeting_time_durations as (
    select k_class_period, sum(meeting_time_duration) as period_duration
    from meeting_times
    group by k_class_period
),
meeting_time_overlaps as (
    select
        a.k_class_period,
        true as has_overlapping_meeting_times
    from meeting_times a
    join meeting_times b
        on a.k_class_period = b.k_class_period
        and a.meeting_time_index < b.meeting_time_index
        and a.military_start_time < b.military_end_time
        and b.military_start_time < a.military_end_time
    group by a.k_class_period
),
meeting_time_running_end as (
    select
        k_class_period,
        meeting_time_index,
        military_start_time,
        military_end_time,
        max(military_end_time) over (
            partition by k_class_period
            order by military_start_time, military_end_time, meeting_time_index
            rows between unbounded preceding and 1 preceding
        ) as previous_max_military_end_time
    from meeting_times
    where military_start_time is not null
      and military_end_time is not null
),
meeting_time_groups as (
    select
        *,
        sum(
            case
                when previous_max_military_end_time is null
                    or military_start_time >= previous_max_military_end_time
                then 1
                else 0
            end
        ) over (
            partition by k_class_period
            order by military_start_time, military_end_time, meeting_time_index
            rows between unbounded preceding and current row
        ) as meeting_time_group
    from meeting_time_running_end
),
merged_meeting_times as (
    select
        k_class_period,
        meeting_time_group,
        min(military_start_time) as military_start_time,
        max(military_end_time) as military_end_time
    from meeting_time_groups
    group by
        k_class_period,
        meeting_time_group
),
unique_meeting_time_durations as (
    select
        k_class_period,
        sum(
            timediff(MINUTE, concat('2020-01-01 ', military_start_time)::timestamp, concat('2020-01-01 ', military_end_time)::timestamp)
        ) as period_unique_duration
    from merged_meeting_times
    group by k_class_period
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
formatted as (
    select 
        class_periods.k_class_period,
        dim_school.k_school,
        class_periods.tenant_code,
        class_periods.school_year,
        class_periods.class_period_name,
        class_periods.is_official_attendance_period,
        {{ edu_edfi_source.json_array_size('v_meeting_times') }} as meeting_time_count,
        sum_meeting_time_durations.period_duration,
        unique_meeting_time_durations.period_unique_duration as period_duration_without_overlap,
        singleton_meeting_times.start_time,
        singleton_meeting_times.end_time,
        {{ json_array_to_struct_array(
            'v_meeting_times',
            [
                ['start_time', 'item:startTime::string'],
                ['end_time', 'item:endTime::string']
            ]
        ) }} as meeting_times,
        coalesce(
            meeting_time_overlaps.has_overlapping_meeting_times,
            false
        ) as has_overlapping_meeting_times

        {#- add any extension columns configured from stg_ef3__class_periods #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__class_periods', flatten=False) }}

        -- custom data sources columns
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
    from class_periods
    left join singleton_meeting_times
        on singleton_meeting_times.k_class_period = class_periods.k_class_period
    left join sum_meeting_time_durations
        on sum_meeting_time_durations.k_class_period = class_periods.k_class_period
    join dim_school
        on class_periods.k_school = dim_school.k_school
    left join meeting_time_overlaps
        on meeting_time_overlaps.k_class_period = class_periods.k_class_period
    left join unique_meeting_time_durations
      on unique_meeting_time_durations.k_class_period = class_periods.k_class_period

    -- custom data sources
    {{ edu_wh.add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='class_periods', join_cols=['k_class_period']) }}
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select *
from formatted
order by tenant_code, k_school, k_class_period