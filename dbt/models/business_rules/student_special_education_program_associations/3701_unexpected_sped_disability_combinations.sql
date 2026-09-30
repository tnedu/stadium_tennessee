{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3701 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
sped_disabilities as (
    select c.*, speds.k_student_program,
        brule.tdoe_error_code as potential_tdoe_error_code,
        brule.tdoe_severity as potential_tdoe_severity
    from {{ ref('stg_ef3__stu_spec_ed__disabilities') }} c
    join brule brule
        on cast(c.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    join {{ ref('stg_ef3__student_special_education_program_associations') }} speds
        on speds.tenant_code = c.tenant_code
        and speds.school_year = c.school_year
        and speds.k_student = c.k_student
        and speds.k_program = c.k_program
        and coalesce(speds.k_lea, 'ZZZ') = coalesce(c.k_lea, 'ZZZ')
        and coalesce(speds.k_school, 'ZZZ') = coalesce(c.k_school, 'ZZZ')
        and speds.program_enroll_begin_date = c.program_enroll_begin_date
),
order_of_disability_counts as (
    select k_student_program, tenant_code, k_student, k_program, 
        k_lea, k_school, ed_org_id, school_year,
        program_enroll_begin_date,
        sum(
            case
                when order_of_disability = 1 then 1
                else 0
            end) as primaries,
        sum(
            case
                when order_of_disability = 2 then 1
                else 0
            end) as secondaries,
        sum(
            case
                when order_of_disability > 2 then 1
                else 0
            end) as others,
        potential_tdoe_error_code, potential_tdoe_severity
    from sped_disabilities 
    group by k_student_program, tenant_code, k_student, k_program, 
        k_lea, k_school, ed_org_id, school_year,
        program_enroll_begin_date, potential_tdoe_error_code, potential_tdoe_severity
),
bad_disabilities as (
    select *,
        case
            when primaries = 0 then 'Missing Primary Disability'
            when primaries > 1 then 'Has too many Primary Disabilities'
            when secondaries > 1 then 'Has too many Secondary Disabilities'
            when others > 0 then 'Has abnormal Disabilities (order of disability >= 3)'
            else 'Some other error'
        end as bad_disability_reason
    from order_of_disability_counts
    where primaries != 1
        or secondaries > 1
        or others > 0
),
errors as (
    select bad.k_student_program, bad.k_program, bad.k_student, bad.k_lea, bad.k_school, bad.school_year,
        bad.program_enroll_begin_date, 
        bad.ed_org_id, stu.student_unique_id, stu.state_student_id as legacy_state_student_id,
        bad.potential_tdoe_error_code as error_code,
        concat('Student ', stu.student_unique_id, ' (', stu.state_student_id, ') at District ', bad.ed_org_id, ' has a SPED Association starting on ', 
            bad.program_enroll_begin_date, ' with bad Disability data: ',
            bad.bad_disability_reason, '.') as error,
        {{ severity_to_severity_code_case_clause('bad.potential_tdoe_severity') }},
        bad.potential_tdoe_severity as tdoe_severity
    from bad_disabilities bad
    join {{ ref('stg_ef3__students') }} stu
        on stu.k_student = bad.k_student
)
select * from errors