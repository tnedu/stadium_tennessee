-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_bell_schedule set not null",
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add primary key (k_bell_schedule, k_class_period)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_bell_schedule) references {{ ref('dim_bell_schedule') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_class_period foreign key (k_class_period) references {{ ref('dim_class_period') }}"
    ]
  )
}}
  
with bell_schedules as (
    select * from {{ ref('dim_bell_schedule') }}
),
bell_schedule_class_periods as (
    select * from {{ ref('stg_ef3__bell_schedules__class_periods') }}
),
bell_schedule_dates as (
    /* Grain: One bell schedule x class period x date. */
    select
        relationships.k_bell_schedule,
        relationships.k_class_period,
        {{ array_explode_value('schedule_date') }}::date as calendar_date
    from bell_schedule_class_periods relationships
    join bell_schedules bell_schedules
        on bell_schedules.k_bell_schedule = relationships.k_bell_schedule
        {{ array_explode(
            'bell_schedules.dates',
            'schedule_date'
        ) }}
),
duplicate_schedule_date_pairs as (
    /*
     * Grain: One pair of bell schedules containing the same class period on the same date.
     *
     * The bell-schedule key comparison evaluates each pair once.
     */
    select
        a.k_class_period,
        a.k_bell_schedule as k_bell_schedule_a,
        b.k_bell_schedule as k_bell_schedule_b,
        a.calendar_date
    from bell_schedule_dates a
    join bell_schedule_dates b
        on a.k_class_period = b.k_class_period
        and a.calendar_date = b.calendar_date
        and a.k_bell_schedule < b.k_bell_schedule
),
duplicate_schedule_date_participants as (
    /*
     * Make the relationship bidirectional so both affected
     * bridge rows carry the diagnostic.
     */
    select
        k_class_period,
        k_bell_schedule_a as k_bell_schedule,
        k_bell_schedule_b as k_duplicate_bell_schedule,
        calendar_date
    from duplicate_schedule_date_pairs
    union
    select
        k_class_period,
        k_bell_schedule_b as k_bell_schedule,
        k_bell_schedule_a as k_duplicate_bell_schedule,
        calendar_date
    from duplicate_schedule_date_pairs
),
duplicate_schedule_date_details as (
    select
        duplicates.k_class_period,
        duplicates.k_bell_schedule,
        duplicates.k_duplicate_bell_schedule,
        other_bell_schedule.bell_schedule_name as duplicate_bell_schedule_name,
        {{ edu_edfi_source.json_array_agg(
            'duplicates.calendar_date',
            order_by='duplicates.calendar_date'
        ) }} as duplicate_dates
    from (
        select distinct
            k_class_period,
            k_bell_schedule,
            k_duplicate_bell_schedule,
            calendar_date
        from duplicate_schedule_date_participants
    ) duplicates
    join bell_schedules other_bell_schedule
        on other_bell_schedule.k_bell_schedule = duplicates.k_duplicate_bell_schedule
    group by
        duplicates.k_class_period,
        duplicates.k_bell_schedule,
        duplicates.k_duplicate_bell_schedule,
        other_bell_schedule.bell_schedule_name
),
duplicate_schedule_date_rollup as (
    select
        k_class_period,
        k_bell_schedule,
        true as has_class_period_duplicate_dates,
        {{ edu_edfi_source.json_array_agg(
            edu_edfi_source.json_object_construct([
                ['k_bell_schedule', 'k_duplicate_bell_schedule'],
                ['bell_schedule_name', 'duplicate_bell_schedule_name'],
                ['duplicate_dates', 'duplicate_dates']
            ])
        ) }} as class_period_duplicate_schedules
    from duplicate_schedule_date_details
    group by
        k_class_period,
        k_bell_schedule
),
joined as (
    select
        relationships.k_bell_schedule,
        relationships.k_class_period,
        relationships.tenant_code,
        relationships.api_year as school_year,
        coalesce(
            issues.has_class_period_duplicate_dates,
            false
        ) as has_class_period_duplicate_dates,
        issues.class_period_duplicate_schedules
    from bell_schedule_class_periods relationships
    left join duplicate_schedule_date_rollup issues
        on issues.k_class_period = relationships.k_class_period
        and issues.k_bell_schedule = relationships.k_bell_schedule
)
select * from joined