{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3703 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
disallowed_secondary_combinations as (
    select 'Secondary Option 1 Consultation' as secondary, 'Option 8 Self-Contained or CDC' as disallowed_primary union
    select 'Secondary Option 1 Consultation' as secondary, 'Option 9 Residential Services' as disallowed_primary union
    select 'Secondary Option 1 Consultation' as secondary, 'Option 10 Hospital / Homebound' as disallowed_primary union

    select 'Secondary Option 2 Consultation' as secondary, 'Option 1 Consultation' as disallowed_primary union
    select 'Secondary Option 2 Consultation' as secondary, 'Option 8 Self-Contained or CDC' as disallowed_primary union
    select 'Secondary Option 2 Consultation' as secondary, 'Option 9 Residential Services' as disallowed_primary union
    select 'Secondary Option 2 Consultation' as secondary, 'Option 10 Hospital / Homebound' as disallowed_primary union

    select 'Secondary Option 3 Consultation' as secondary, 'Option 1 Consultation' as disallowed_primary union
    select 'Secondary Option 3 Consultation' as secondary, 'Option 2 Direct Services' as disallowed_primary union
    select 'Secondary Option 3 Consultation' as secondary, 'Option 8 Self-Contained or CDC' as disallowed_primary union
    select 'Secondary Option 3 Consultation' as secondary, 'Option 9 Residential Services' as disallowed_primary union
    select 'Secondary Option 3 Consultation' as secondary, 'Option 10 Hospital / Homebound' as disallowed_primary union

    select 'Secondary Option 4 Consultation' as secondary, 'Option 1 Consultation' as disallowed_primary union
    select 'Secondary Option 4 Consultation' as secondary, 'Option 2 Direct Services' as disallowed_primary union
    select 'Secondary Option 4 Consultation' as secondary, 'Option 3 Direct Services' as disallowed_primary union
    select 'Secondary Option 4 Consultation' as secondary, 'Option 7 Direct Services' as disallowed_primary union
    select 'Secondary Option 4 Consultation' as secondary, 'Option 8 Self-Contained or CDC' as disallowed_primary union
    select 'Secondary Option 4 Consultation' as secondary, 'Option 9 Residential Services' as disallowed_primary union
    select 'Secondary Option 4 Consultation' as secondary, 'Option 10 Hospital / Homebound' as disallowed_primary union

    select 'Secondary Option 5 Consultation' as secondary, 'Option 1 Consultation' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 2 Direct Services' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 3 Direct Services' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 4 Direct Services' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 6 Ancillary Services' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 7 Direct Services' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 8 Self-Contained or CDC' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 9 Residential Services' as disallowed_primary union
    select 'Secondary Option 5 Consultation' as secondary, 'Option 10 Hospital / Homebound' as disallowed_primary
),
sped_program_services as (
    select c.*,
        brule.tdoe_error_code as potential_tdoe_error_code,
        brule.tdoe_severity as potential_tdoe_severity
    from {{ ref('stg_ef3__stu_spec_ed__program_services') }} c
    join brule brule
        on cast(c.api_year as int) between brule.error_school_year_start and brule.error_school_year_end
),
secondaries_on_disallowed_primaries as (
    select secondaries.*,
        primaries.program_service as primary_program_service, primaries.service_begin_date as primary_service_begin_date, 
        primaries.service_end_date as primary_service_end_date
    from sped_program_services secondaries
    join disallowed_secondary_combinations disallowed
        on disallowed.secondary = secondaries.program_service
    join sped_program_services primaries
        on primaries.k_student_program = secondaries.k_student_program
        and primaries.primary_indicator = true
        and primaries.program_service = disallowed.disallowed_primary
    where secondaries.primary_indicator = false
        and secondaries.service_begin_date <= coalesce(primaries.service_end_date, to_date(concat(secondaries.api_year, '-06-30')))
        and coalesce(secondaries.service_end_date, to_date(concat(secondaries.api_year, '-06-30'))) >= primaries.service_begin_date
),
errors as (
    select bad.k_student_program, bad.k_program, bad.k_student, bad.k_lea, bad.k_school, bad.api_year as school_year, 
        bad.program_enroll_begin_date,
        bad.ed_org_id, stu.student_unique_id, stu.state_student_id as legacy_state_student_id,
        bad.potential_tdoe_error_code as error_code,
        concat('Student ', stu.student_unique_id, ' (', stu.state_student_id, ') at District ', bad.ed_org_id, ' has Secondary SPED Program Service "', 
            concat(bad.program_service, ' [', bad.service_begin_date, ' - ', coalesce(bad.service_end_date, 'no end date'), ']" '), 
            'on a disallowed Primary SPED Program Service " ', 
            concat(bad.primary_program_service, ' [', bad.primary_service_begin_date, ' - ', coalesce(bad.primary_service_end_date, 'no end date'), ']".')) as error,
        {{ severity_to_severity_code_case_clause('bad.potential_tdoe_severity') }},
        bad.potential_tdoe_severity as tdoe_severity
    from secondaries_on_disallowed_primaries bad
    join {{ ref('stg_ef3__students') }} stu
        on stu.k_student = bad.k_student
)
select * from errors