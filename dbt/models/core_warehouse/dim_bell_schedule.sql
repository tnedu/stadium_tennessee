-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_bell_schedule set not null",
        "alter table {{ this }} add primary key (k_bell_schedule)",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}

with bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
bell_schedule_dates as (
    select * from {{ ref('stg_ef3__bell_schedules__dates') }}
),
grade_levels as (
    select
        k_bell_schedule,
        array_agg(grade_level) as grade_levels
    from {{ ref('stg_ef3__bell_schedules__grade_levels') }}
    group by k_bell_schedule
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
calendar_dates as (
    select * from {{ ref('dim_calendar_date') }}
),
bell_schedule_date_status as (
    /*
     * Grain: One configured date per bell schedule.
     *
     * dim_calendar_date is intentionally left joined because a configured
     * bell-schedule date can be absent from the school calendar.
     */
    select
        bell_schedule_dates.k_bell_schedule,
        bell_schedule_dates.calendar_date,
        calendar_dates.k_calendar_date,
        calendar_dates.is_school_day
    from bell_schedule_dates
    join bell_schedules
        on bell_schedules.k_bell_schedule = bell_schedule_dates.k_bell_schedule
    join dim_school
        on dim_school.k_school = bell_schedules.k_school
    left join calendar_dates
        on calendar_dates.k_school = dim_school.k_school
        and calendar_dates.calendar_date = bell_schedule_dates.calendar_date
),
dates as (
    select
        k_bell_schedule,
        {{ edu_edfi_source.json_array_agg(
            'calendar_date',
            order_by='calendar_date'
        ) }} as dates,
        concat(
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 1
                        then 1 else 0
                    end
                ) = 1
                then 'M' else ''
            end,
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 2
                        then 1 else 0
                    end
                ) = 1
                then 'T' else ''
            end,
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 3
                        then 1 else 0
                    end
                ) = 1
                then 'W' else ''
            end,
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 4
                        then 1 else 0
                    end
                ) = 1
                then 'R' else ''
            end,
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 5
                        then 1 else 0
                    end
                ) = 1
                then 'F' else ''
            end,
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 6
                        then 1 else 0
                    end
                ) = 1
                then 'S' else ''
            end,
            case
                when max(
                    case
                        when {{ day_of_week('calendar_date') }} = 7
                        then 1 else 0
                    end
                ) = 1
                then 'U' else ''
            end
        ) as inferred_weekdays,
        /*
         * These dates exist in dim_calendar_date, but the calendar
         * explicitly identifies them as non-school days.
         */
        case
            when max(
                case
                    when k_calendar_date is not null
                        and is_school_day = false
                    then 1
                    else 0
                end
            ) = 1
            then {{ edu_edfi_source.json_array_agg(
                "case
                    when k_calendar_date is not null
                        and is_school_day = false
                    then calendar_date
                end",
                order_by='calendar_date'
            ) }}
        end as non_school_dates,
        /*
         * These dates are configured on the bell schedule but have no
         * corresponding school/date row in dim_calendar_date.
         */
        case
            when max(
                case
                    when k_calendar_date is null
                    then 1
                    else 0
                end
            ) = 1
            then {{ edu_edfi_source.json_array_agg(
                "case
                    when k_calendar_date is null
                    then calendar_date
                end",
                order_by='calendar_date'
            ) }}
        end as missing_calendar_dates
    from bell_schedule_date_status
    group by k_bell_schedule
),
formatted as (
    select 
        bell_schedules.k_bell_schedule,
        dim_school.k_school,
        bell_schedules.tenant_code,
        bell_schedules.school_year,
        bell_schedules.bell_schedule_name,
        bell_schedules.alternate_day_name,
        bell_schedules.start_time,
        bell_schedules.end_time,
        bell_schedules.total_instructional_time,
        grade_levels.grade_levels,
        dates.dates,
        dates.inferred_weekdays,
        array_min(dates.dates)::date as first_date,
        array_max(dates.dates)::date as last_date,
        size(dates.dates) as date_count,
        dates.non_school_dates,
        dates.missing_calendar_dates

        {#- add any extension columns configured from stg_ef3__bell_schedules #}
        {{ edu_edfi_source.extract_extension(
            model_name='stg_ef3__bell_schedules',
            flatten=False
        ) }}

    from bell_schedules
    left join grade_levels
        on grade_levels.k_bell_schedule = bell_schedules.k_bell_schedule
    left join dates
        on dates.k_bell_schedule = bell_schedules.k_bell_schedule
    join dim_school
        on bell_schedules.k_school = dim_school.k_school
)
select *
from formatted
order by tenant_code, k_school, k_bell_schedule