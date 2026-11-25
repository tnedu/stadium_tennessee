{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_course_section set not null"
    ]
  )
}}

with section_class_period_schedules as (
    select * from {{ ref('fct_course_section_class_period_schedule') }}
),
class_period_meeting_times as (
    select * from {{ ref('dtl_class_period_meeting_time') }}
),
sections as (
    /* Grain: One row per course section / session. */
    select distinct
        k_school,
        k_course_section,
        k_course,
        tenant_code,
        school_year,
        section_id,
        local_course_code,
        course_code,
        session_name,
        has_students
    from section_class_period_schedules
),
daily_schedule_rows as (
    /* Grain: One course section x date x class period x bell schedule. */
    select
        schedules.k_course_section,
        schedules.k_class_period,
        schedules.class_period_name,
        schedules.period_duration,
        schedules.period_duration_without_overlap,
        schedules.k_bell_schedule,
        schedules.bell_schedule_name,
        {{ array_explode_value('schedule_date') }}::date as calendar_date
    from section_class_period_schedules schedules
        {{ array_explode(
            'schedules.dates',
            'schedule_date'
        ) }}
),
daily_meeting_time_rows as (
    /* Grain: One section x date x bell schedule x class period x meeting time. */
    select distinct
        schedules.k_course_section,
        schedules.calendar_date,
        schedules.k_bell_schedule,
        schedules.bell_schedule_name,
        schedules.k_class_period,
        schedules.class_period_name,
        schedules.period_duration,
        schedules.period_duration_without_overlap,
        meeting_times.k_class_period_meeting_time,
        meeting_times.start_time,
        meeting_times.end_time,
        meeting_times.military_start_time,
        meeting_times.military_end_time,
        meeting_times.meeting_time_duration
    from daily_schedule_rows schedules
    join class_period_meeting_times meeting_times
        on meeting_times.k_class_period = schedules.k_class_period
    where meeting_times.military_start_time is not null
        and meeting_times.military_end_time is not null
),
daily_class_period_meeting_time_rows as (
    /*
     * Presentation grain:
     * One course section x date x class period x meeting time.
     *
     * Bell-schedule duplication is intentionally removed here so the
     * same meeting time is not displayed once per applicable bell schedule.
     *
     * This does not affect scheduled_minutes, which is calculated from
     * the original daily_meeting_time_rows CTE.
     */
    select distinct
        k_course_section,
        calendar_date,

        k_class_period,
        class_period_name,

        period_duration,
        period_duration_without_overlap,

        k_class_period_meeting_time,

        start_time,
        end_time,

        military_start_time,
        military_end_time,

        meeting_time_duration

    from daily_meeting_time_rows
),
daily_meeting_time_rollup as (
    /*
     * Grain:
     * One course section x date x class period.
     */
    select
        k_course_section,
        calendar_date,

        k_class_period,
        class_period_name,

        period_duration,
        period_duration_without_overlap,

        {{ edu_edfi_source.json_array_agg(
            'meeting_time',
            order_by='military_start_time'
        ) }} as meeting_times

    from (
        select
            k_course_section,
            calendar_date,

            k_class_period,
            class_period_name,

            period_duration,
            period_duration_without_overlap,

            military_start_time,

            {{ edu_edfi_source.json_object_construct([
                ['k_class_period_meeting_time', 'k_class_period_meeting_time'],
                ['start_time', 'start_time'],
                ['end_time', 'end_time'],
                ['meeting_time_duration', 'meeting_time_duration']
            ]) }} as meeting_time

        from daily_class_period_meeting_time_rows
    ) meeting_time_values

    group by
        k_course_section,
        calendar_date,
        k_class_period,
        class_period_name,
        period_duration,
        period_duration_without_overlap
),
daily_class_period_time_bounds as (
    /* Grain: One course section x date x class period. */
    select
        k_course_section,
        calendar_date,
        k_class_period,

        min(military_start_time) as first_start_time

    from daily_class_period_meeting_time_rows

    group by
        k_course_section,
        calendar_date,
        k_class_period
),
daily_class_period_bell_schedule_values as (
    /*
     * Grain:
     * One course section x date x class period x bell schedule.
     */
    select distinct
        k_course_section,
        calendar_date,
        k_class_period,

        bell_schedule_name,

        {{ edu_edfi_source.json_object_construct([
            ['k_bell_schedule', 'k_bell_schedule'],
            ['bell_schedule_name', 'bell_schedule_name']
        ]) }} as bell_schedule

    from daily_schedule_rows
),
daily_class_period_bell_schedule_rollup as (
    /*
     * Grain:
     * One course section x date x class period.
     */
    select
        k_course_section,
        calendar_date,
        k_class_period,

        {{ edu_edfi_source.json_array_agg(
            'bell_schedule',
            order_by='bell_schedule_name'
        ) }} as bell_schedules

    from daily_class_period_bell_schedule_values

    group by
        k_course_section,
        calendar_date,
        k_class_period
),
daily_class_period_values as (
    /* Grain: One complete class-period object per section and date. */
    select
        meeting_times.k_course_section,
        meeting_times.calendar_date,

        time_bounds.first_start_time,
        meeting_times.class_period_name,

        {{ edu_edfi_source.json_object_construct([
            ['k_class_period', 'meeting_times.k_class_period'],
            ['class_period_name', 'meeting_times.class_period_name'],
            ['period_duration', 'meeting_times.period_duration'],
            ['period_duration_without_overlap', 'meeting_times.period_duration_without_overlap'],
            ['meeting_times', 'meeting_times.meeting_times'],
            ['bell_schedules', 'bell_schedules.bell_schedules']
        ]) }} as class_period

    from daily_meeting_time_rollup meeting_times

    join daily_class_period_time_bounds time_bounds
        on time_bounds.k_course_section =
            meeting_times.k_course_section
        and time_bounds.calendar_date =
            meeting_times.calendar_date
        and time_bounds.k_class_period =
            meeting_times.k_class_period

    join daily_class_period_bell_schedule_rollup bell_schedules
        on bell_schedules.k_course_section =
            meeting_times.k_course_section
        and bell_schedules.calendar_date =
            meeting_times.calendar_date
        and bell_schedules.k_class_period =
            meeting_times.k_class_period
),
daily_class_period_rollup as (
    /* Grain: One course section x calendar date. */
    select
        k_course_section,
        calendar_date,

        {{ edu_edfi_source.json_array_agg(
            'class_period',
            order_by='first_start_time, class_period_name'
        ) }} as class_periods

    from daily_class_period_values

    group by
        k_course_section,
        calendar_date
),
daily_schedule_metrics as (
    /*
     * Grain: One course section x calendar date.
     *
     * scheduled_minutes intentionally includes overlapping intervals.
     * The time bounds identify the earliest start and latest end, but do
     * not imply continuous occupancy between those times.
     */
    select
        k_course_section,
        calendar_date,
        min(military_start_time) as first_start_time,
        sum(meeting_time_duration) as scheduled_minutes
    from daily_meeting_time_rows
    group by
        k_course_section,
        calendar_date
),
daily_meeting_time_running_end as (
    select
        k_course_section,
        calendar_date,
        k_bell_schedule,
        k_class_period,
        k_class_period_meeting_time,
        military_start_time,
        military_end_time,
        max(military_end_time) over (
            partition by
                k_course_section,
                calendar_date
            order by
                military_start_time,
                military_end_time,
                k_bell_schedule,
                k_class_period,
                k_class_period_meeting_time
            rows between unbounded preceding and 1 preceding
        ) as previous_max_military_end_time
    from daily_meeting_time_rows
),
daily_meeting_time_groups as (
    select *,
        sum(
            case
                when previous_max_military_end_time is null
                    or military_start_time >= previous_max_military_end_time
                then 1
                else 0
            end
        ) over (
            partition by
                k_course_section,
                calendar_date
            order by
                military_start_time,
                military_end_time,
                k_bell_schedule,
                k_class_period,
                k_class_period_meeting_time
            rows between unbounded preceding and current row
        ) as meeting_time_group
    from daily_meeting_time_running_end
),
daily_merged_meeting_times as (
    select
        k_course_section,
        calendar_date,
        meeting_time_group,
        min(military_start_time) as military_start_time,
        max(military_end_time) as military_end_time
    from daily_meeting_time_groups
    group by
        k_course_section,
        calendar_date,
        meeting_time_group
),
daily_unique_duration as (
    /*
     * This is the duration of the union of every meeting-time interval
     * belonging to the section on this date.
     */
    select
        k_course_section,
        calendar_date,
        sum(
            timediff(
                minute,
                concat(
                    '2020-01-01 ',
                    military_start_time
                )::timestamp,
                concat(
                    '2020-01-01 ',
                    military_end_time
                )::timestamp
            )
        ) as scheduled_minutes_without_overlap
    from daily_merged_meeting_times
    group by
        k_course_section,
        calendar_date
),
daily_schedule_values as (
    /*
     * Grain:
     * One complete daily schedule value per course section and date.
     */
    select
        class_periods.k_course_section,
        class_periods.calendar_date,

        metrics.first_start_time,
        metrics.scheduled_minutes,

        unique_duration.scheduled_minutes_without_overlap,

        {{ edu_edfi_source.json_object_construct([
            ['calendar_date', 'class_periods.calendar_date'],
            ['first_start_time', 'metrics.first_start_time'],
            ['class_periods', 'class_periods.class_periods'],
            ['scheduled_minutes', 'metrics.scheduled_minutes'],
            ['scheduled_minutes_without_overlap', 'unique_duration.scheduled_minutes_without_overlap']
        ]) }} as daily_schedule_value

    from daily_class_period_rollup class_periods

    join daily_schedule_metrics metrics
        on metrics.k_course_section =
            class_periods.k_course_section

        and metrics.calendar_date =
            class_periods.calendar_date

    join daily_unique_duration unique_duration
        on unique_duration.k_course_section =
            class_periods.k_course_section

        and unique_duration.calendar_date =
            class_periods.calendar_date
),
section_daily_schedule_rollup as (
    select
        k_course_section,
        {{ edu_edfi_source.json_array_agg(
            'daily_schedule_value',
            order_by='calendar_date'
        ) }} as daily_schedule,
        min(calendar_date) as first_date,
        max(calendar_date) as last_date,
        count(*) as date_count
    from daily_schedule_values
    group by k_course_section
),
section_duration_rollup as (
    select
        k_course_section,
        sum(scheduled_minutes) as total_scheduled_minutes,
        sum(scheduled_minutes_without_overlap) as total_scheduled_minutes_without_overlap
    from daily_schedule_values
    group by k_course_section
),
exploded_non_school_dates as (
    select
        schedules.k_course_section,
        {{ array_explode_value('non_school_date') }}::date as non_school_date
    from section_class_period_schedules schedules
        {{ array_explode(
            'schedules.non_school_dates',
            'non_school_date'
        ) }}
),
section_non_school_dates as (
    select distinct
        k_course_section,
        non_school_date
    from exploded_non_school_dates
    where non_school_date is not null
),
section_non_school_date_rollup as (
    select
        k_course_section,
        {{ edu_edfi_source.json_array_agg(
            'non_school_date',
            order_by='non_school_date'
        ) }} as non_school_dates
    from section_non_school_dates
    group by k_course_section
),
exploded_missing_calendar_dates as (
    select
        schedules.k_course_section,
        {{ array_explode_value('missing_calendar_date') }}::date as missing_calendar_date
    from section_class_period_schedules schedules
        {{ array_explode(
            'schedules.missing_calendar_dates',
            'missing_calendar_date'
        ) }}
),
section_missing_calendar_dates as (
    select distinct
        k_course_section,
        missing_calendar_date
    from exploded_missing_calendar_dates
    where missing_calendar_date is not null
),
section_missing_calendar_date_rollup as (
    select
        k_course_section,
        {{ edu_edfi_source.json_array_agg(
            'missing_calendar_date',
            order_by='missing_calendar_date'
        ) }} as missing_calendar_dates
    from section_missing_calendar_dates
    group by k_course_section
),
section_meeting_time_issues as (
    select
        k_course_section,
        case
            when max(
                case
                    when has_overlapping_meeting_times then 1
                    else 0
                end
            ) = 1
            then true
            else false
        end as has_overlapping_meeting_times
    from section_class_period_schedules
    group by k_course_section
),
section_class_period_overlap_details as (
    /*
     * Grain:
     * One course section x current schedule row x overlapping
     * class-period schedule.
     */
    select
        schedules.k_course_section,
        schedules.k_course_section_class_period_schedule,
        schedules.k_class_period,
        schedules.class_period_name,
        schedules.k_bell_schedule,
        schedules.bell_schedule_name,
        {{ array_explode_value('overlapping_class_period') }} as overlapping_class_period
    from section_class_period_schedules schedules
        {{ array_explode(
            'schedules.overlapping_class_periods',
            'overlapping_class_period'
        ) }}
    where schedules.has_class_period_overlap
),
section_class_period_overlap_pairs as (
    /*
     * Each overlap is represented on both affected lower-grain rows.
     *
     * Keep one canonical direction:
     *   1. lower class-period key wins
     *   2. if needed, lower bell-schedule key breaks a tie
     */
    select
        k_course_section,
        k_class_period,
        class_period_name,
        k_bell_schedule,
        bell_schedule_name,
        overlapping_class_period
    from section_class_period_overlap_details
    where
        k_class_period < overlapping_class_period.k_class_period
        or (
            k_class_period = overlapping_class_period.k_class_period
            and k_bell_schedule < overlapping_class_period.k_bell_schedule
        )
),
section_class_period_overlap_rollup as (
    select
        k_course_section,
        {{ edu_edfi_source.json_array_agg(
            'class_period_overlap'
        ) }} as overlapping_class_periods
    from (
        select
            k_course_section,
            {{ edu_edfi_source.json_object_construct([
                ['k_class_period', 'k_class_period'],
                ['class_period_name', 'class_period_name'],
                ['k_bell_schedule', 'k_bell_schedule'],
                ['bell_schedule_name', 'bell_schedule_name'],
                ['overlaps_with', 'overlapping_class_period']
            ]) }} as class_period_overlap
        from section_class_period_overlap_pairs
    ) overlap_pairs
    group by k_course_section
),
section_class_period_duplicate_details as (
    /*
     * Grain:
     * One course section x current class-period/bell-schedule row
     * x conflicting bell schedule.
     */
    select
        schedules.k_course_section,
        schedules.k_class_period,
        schedules.class_period_name,
        schedules.k_bell_schedule,
        schedules.bell_schedule_name,
        {{ array_explode_value('duplicate_schedule') }}
            as duplicate_schedule
    from section_class_period_schedules schedules
        {{ array_explode(
            'schedules.class_period_duplicate_schedules',
            'duplicate_schedule'
        ) }}
    where schedules.has_class_period_duplicate_dates
),
section_class_period_duplicate_pairs as (
    select
        k_course_section,
        k_class_period,
        class_period_name,
        k_bell_schedule,
        bell_schedule_name,
        duplicate_schedule
    from section_class_period_duplicate_details
    where k_bell_schedule < duplicate_schedule.k_bell_schedule
),
section_class_period_duplicate_pair_values as (
    select
        k_course_section,
        k_class_period,
        class_period_name,
        bell_schedule_name,
        duplicate_schedule.bell_schedule_name as duplicate_bell_schedule_name,

        {{ edu_edfi_source.json_object_construct([
            ['k_bell_schedule', 'k_bell_schedule'],
            ['bell_schedule_name', 'bell_schedule_name'],
            ['duplicates_with', 'duplicate_schedule']
        ]) }} as duplicate_bell_schedule_pair

    from section_class_period_duplicate_pairs
),
section_class_period_duplicate_pair_rollup as (
    select
        k_course_section,
        k_class_period,
        class_period_name,

        {{ edu_edfi_source.json_array_agg(
            'duplicate_bell_schedule_pair',
            order_by='bell_schedule_name, duplicate_bell_schedule_name'
        ) }} as duplicate_bell_schedule_pairs

    from section_class_period_duplicate_pair_values

    group by
        k_course_section,
        k_class_period,
        class_period_name
),
section_class_period_duplicate_rollup as (
    select
        k_course_section,

        {{ edu_edfi_source.json_array_agg(
            'class_period_duplicate_schedule',
            order_by='class_period_name'
        ) }} as class_period_duplicate_schedules

    from (
        select
            k_course_section,
            class_period_name,

            {{ edu_edfi_source.json_object_construct([
                ['k_class_period', 'k_class_period'],
                ['class_period_name', 'class_period_name'],
                ['duplicate_bell_schedule_pairs', 'duplicate_bell_schedule_pairs']
            ]) }} as class_period_duplicate_schedule

        from section_class_period_duplicate_pair_rollup
    ) class_period_duplicates

    group by k_course_section
),
formatted as (
    select
        sections.k_course_section,
        sections.k_school,
        sections.k_course,
        sections.tenant_code,
        sections.school_year,
        sections.section_id,
        sections.local_course_code,
        sections.course_code,
        sections.session_name,
        sections.has_students,
        section_daily_schedule_rollup.daily_schedule,
        section_daily_schedule_rollup.first_date,
        section_daily_schedule_rollup.last_date,
        section_daily_schedule_rollup.date_count,
        section_duration_rollup.total_scheduled_minutes,
        section_duration_rollup.total_scheduled_minutes_without_overlap,
        case
            when section_daily_schedule_rollup.date_count > 0
            then cast(
                cast(
                    section_duration_rollup.total_scheduled_minutes
                    as decimal(18, 2)
                )
                / section_daily_schedule_rollup.date_count
                as decimal(10, 2)
            )
        end as avg_scheduled_minutes_per_day,
        case
            when section_daily_schedule_rollup.date_count > 0
            then cast(
                cast(
                    section_duration_rollup.total_scheduled_minutes_without_overlap
                    as decimal(18, 2)
                )
                / section_daily_schedule_rollup.date_count
                as decimal(10, 2)
            )
        end as avg_scheduled_minutes_without_overlap_per_day,
        coalesce(
            section_meeting_time_issues.has_overlapping_meeting_times,
            false
        ) as has_overlapping_meeting_times,
        section_non_school_date_rollup.non_school_dates,
        section_missing_calendar_date_rollup.missing_calendar_dates,
        case
            when section_class_period_duplicate_rollup.k_course_section
                is not null
            then true
            else false
        end as has_class_period_duplicate_dates,
        section_class_period_duplicate_rollup.class_period_duplicate_schedules,
        case
            when section_class_period_overlap_rollup.k_course_section
                is not null
            then true
            else false
        end as has_class_period_overlap,
        section_class_period_overlap_rollup.overlapping_class_periods
    from sections
    left join section_daily_schedule_rollup
        on section_daily_schedule_rollup.k_course_section = sections.k_course_section
    left join section_duration_rollup
        on section_duration_rollup.k_course_section = sections.k_course_section
    left join section_non_school_date_rollup
        on section_non_school_date_rollup.k_course_section = sections.k_course_section
    left join section_missing_calendar_date_rollup
        on section_missing_calendar_date_rollup.k_course_section = sections.k_course_section
    left join section_meeting_time_issues
        on section_meeting_time_issues.k_course_section = sections.k_course_section
    left join section_class_period_duplicate_rollup
        on section_class_period_duplicate_rollup.k_course_section = sections.k_course_section
    left join section_class_period_overlap_rollup
        on section_class_period_overlap_rollup.k_course_section = sections.k_course_section
)
select * from formatted