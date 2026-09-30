{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3700 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
sped_program_services as (
    select c.*,
        brule.tdoe_error_code as potential_tdoe_error_code,
        brule.tdoe_severity as potential_tdoe_severity
    from {{ ref('stg_ef3__stu_spec_ed__program_services') }} c
    join brule brule
        on cast(c.api_year as int) between brule.error_school_year_start and brule.error_school_year_end
),
primaries as (
    select *
    from sped_program_services
    where primary_indicator = true
),
primaries_string_agged as (
    select k_student_program,
        string_agg(concat(program_service, ' [', service_begin_date, ' - ', coalesce(service_end_date, 'no end date'), ']'), ', ') as all_primaries
    from primaries
    group by k_student_program
),
primaries_ordered as (
    select k_student_program, service_begin_date, service_end_date,
        max(service_end_date) over (
            partition by k_student_program
            order by service_begin_date, service_end_date
            rows between unbounded preceding and 1 preceding
        ) as prev_end_date
    from primaries
),
primaries_islands as (
    select *,
        case
            when prev_end_date is null then 1
            -- gap of at least 1 day = new island
            when service_begin_date > date_add(prev_end_date, 1) then 1
            else 0
        end as is_new_island
    from primaries_ordered
), 
primaries_island_ids as (
    select *,
        sum(is_new_island) over (
            partition by k_student_program
            order by service_begin_date, service_end_date
            rows between unbounded preceding and current row
        ) as island_id
    from primaries_islands
),
primaries_aggregated as (
    select * except(service_begin_date, service_end_date),
        min(service_begin_date) as service_begin_date,
        max(service_end_date) as service_end_date
    from primaries_island_ids
    group by all
),
bad_secondaries as (
    select distinct secondaries.k_student_program, secondaries.k_student, secondaries.k_program, 
        secondaries.k_lea, secondaries.k_school, secondaries.ed_org_id, secondaries.api_year as school_year,
        secondaries.program_enroll_begin_date,
        secondaries.program_service as secondary_program_service, 
        secondaries.service_begin_date as secondary_begin_date, 
        secondaries.service_end_date as secondary_end_date,
        secondaries.potential_tdoe_error_code as tdoe_error_code,
        secondaries.potential_tdoe_severity as tdoe_severity
    from sped_program_services secondaries
    join primaries_aggregated primaries
        on primaries.k_student_program = secondaries.k_student_program
    where secondaries.primary_indicator = false
        and (
            secondaries.service_begin_date < primaries.service_begin_date
            or coalesce(secondaries.service_end_date, to_date(concat(secondaries.api_year, '-06-30'))) > 
                coalesce(primaries.service_end_date, to_date(concat(secondaries.api_year, '-06-30')))
        )
),
errors as (
    select bad.k_student_program, bad.k_program, bad.k_student, bad.k_lea, bad.k_school, bad.school_year, 
        bad.program_enroll_begin_date,
        bad.ed_org_id, stu.student_unique_id, stu.state_student_id as legacy_state_student_id,
        bad.tdoe_error_code as error_code,
        concat('Student ', stu.student_unique_id, ' (', stu.state_student_id, ') at District ', bad.ed_org_id, ' has Secondary SPED Program Service "', 
            concat(bad.secondary_program_service, ' [', bad.secondary_begin_date, ' - ', coalesce(bad.secondary_end_date, 'no end date'), ']" '), 
            'that falls outside of their Primary SPED Program Services: ', 
            primaries.all_primaries, '.') as error,
        {{ severity_to_severity_code_case_clause('bad.tdoe_severity') }},
        bad.tdoe_severity as tdoe_severity
    from bad_secondaries bad
    join primaries_string_agged primaries
        on primaries.k_student_program = bad.k_student_program
    join {{ ref('stg_ef3__students') }} stu
        on stu.k_student = bad.k_student
)
select * from errors