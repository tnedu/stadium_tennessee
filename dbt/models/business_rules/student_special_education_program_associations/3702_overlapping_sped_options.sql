{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3702 %}

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
overlapping_internal_program_services as (
    select serviceA.k_student_program as serviceA_k_student_program, serviceB.k_student_program as serviceB_k_student_program,
        serviceA.tenant_code, serviceA.k_student, serviceA.k_lea, serviceA.k_school, serviceA.k_program, serviceA.api_year as school_year,
        serviceA.primary_indicator, serviceA.potential_tdoe_error_code, serviceA.potential_tdoe_severity, serviceA.ed_org_id,
        serviceA.program_enroll_begin_date as serviceA_program_enroll_begin_date, serviceA.program_service as serviceA_program_service,
        serviceA.service_begin_date as serviceA_service_begin_date, serviceA.service_end_date as serviceA_service_end_date,
        serviceB.program_enroll_begin_date as serviceB_program_enroll_begin_date, serviceB.program_service as serviceB_program_service,
        serviceB.service_begin_date as serviceB_service_begin_date, serviceB.service_end_date as serviceB_service_end_date
    from sped_program_services serviceA
    join sped_program_services serviceB
        on serviceB.k_student_program = serviceA.k_student_program
        and serviceB.primary_indicator = serviceA.primary_indicator
        and serviceA.service_begin_date < serviceB.service_begin_date
    where serviceA.service_begin_date <= coalesce(serviceB.service_end_date, to_date(concat(serviceA.api_year, '-06-30')))
        and coalesce(serviceA.service_end_date, to_date(concat(serviceA.api_year, '-06-30'))) >= serviceB.service_begin_date
),
overlapping_external_program_services as (
    select serviceA.k_student_program as serviceA_k_student_program, serviceB.k_student_program as serviceB_k_student_program,
        serviceA.tenant_code, serviceA.k_student, serviceA.k_lea, serviceA.k_school, serviceA.k_program, serviceA.api_year as school_year,
        serviceA.primary_indicator, serviceA.potential_tdoe_error_code, serviceA.potential_tdoe_severity, serviceA.ed_org_id,
        serviceA.program_enroll_begin_date as serviceA_program_enroll_begin_date, serviceA.program_service as serviceA_program_service,
        serviceA.service_begin_date as serviceA_service_begin_date, serviceA.service_end_date as serviceA_service_end_date,
        serviceB.program_enroll_begin_date as serviceB_program_enroll_begin_date, serviceB.program_service as serviceB_program_service,
        serviceB.service_begin_date as serviceB_service_begin_date, serviceB.service_end_date as serviceB_service_end_date
    from sped_program_services serviceA
    join sped_program_services serviceB
        on serviceB.k_student_program != serviceA.k_student_program
        /* Has to be different Sped Assoc but for the same Student/Location. */
        and serviceB.api_year = serviceA.api_year
        and serviceB.k_student = serviceA.k_student
        and coalesce(serviceB.k_lea, 'ZZZ') = coalesce(serviceB.k_lea, 'ZZZ')
        and coalesce(serviceB.k_school, 'ZZZ') = coalesce(serviceB.k_school, 'ZZZ')
        and serviceB.k_program = serviceA.k_program
        and serviceB.primary_indicator = serviceA.primary_indicator
        and serviceA.program_enroll_begin_date < serviceB.program_enroll_begin_date
    where serviceA.service_begin_date <= coalesce(serviceB.service_end_date, to_date(concat(serviceA.api_year, '-06-30')))
        and coalesce(serviceA.service_end_date, to_date(concat(serviceA.api_year, '-06-30'))) >= serviceB.service_begin_date
),
errors as (
    select bad.serviceA_k_student_program, bad.k_program, bad.k_student, bad.k_lea, bad.k_school, bad.school_year, 
        bad.serviceA_program_enroll_begin_date,
        bad.ed_org_id, stu.student_unique_id, stu.state_student_id as legacy_state_student_id,
        bad.potential_tdoe_error_code as error_code,
        concat('Student ', stu.student_unique_id, ' (', stu.state_student_id, ') at District ', bad.ed_org_id, ' has Overlapping SPED Program Services ', 
            'on the Sped Association starting on ', bad.serviceA_program_enroll_begin_date, ': ',
            bad.serviceA_program_service, ' [', bad.serviceA_service_begin_date, ' - ', coalesce(bad.serviceA_service_end_date, 'no end date'), '] ', 
            'overlaps ', 
            bad.serviceB_program_service, ' [', bad.serviceB_service_begin_date, ' - ', coalesce(bad.serviceB_service_end_date, 'no end date'), '] ', 
            '.') as error,
        {{ severity_to_severity_code_case_clause('bad.potential_tdoe_severity') }},
        bad.potential_tdoe_severity as tdoe_severity
    from overlapping_internal_program_services bad
    join {{ ref('stg_ef3__students') }} stu
        on stu.k_student = bad.k_student
    union
    select bad.serviceA_k_student_program, bad.k_program, bad.k_student, bad.k_lea, bad.k_school, bad.school_year, 
        bad.serviceA_program_enroll_begin_date,
        bad.ed_org_id, stu.student_unique_id, stu.state_student_id as legacy_state_student_id,
        bad.potential_tdoe_error_code as error_code,
        concat('Student ', stu.student_unique_id, ' (', stu.state_student_id, ') at District ', bad.ed_org_id, ' has Overlapping SPED Program Services ', 
            'on two SPED Association Records: ', 
            'Program starting on ', bad.serviceA_program_enroll_begin_date, 
            ' with ', bad.serviceA_program_service, ' [', bad.serviceA_service_begin_date, ' - ', coalesce(bad.serviceA_service_end_date, 'no end date'), '] ', 
            ' overlaps ', 
            'Program starting on ', bad.serviceB_program_enroll_begin_date, 
            ' with ', bad.serviceB_program_service, ' [', bad.serviceB_service_begin_date, ' - ', coalesce(bad.serviceB_service_end_date, 'no end date'), '] ', 
            '.') as error,
        {{ severity_to_severity_code_case_clause('bad.potential_tdoe_severity') }},
        bad.potential_tdoe_severity as tdoe_severity
    from overlapping_external_program_services bad
    join {{ ref('stg_ef3__students') }} stu
        on stu.k_student = bad.k_student
)
select * from errors