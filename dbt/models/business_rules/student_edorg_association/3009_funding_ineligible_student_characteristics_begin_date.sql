{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}
{% set error_code = 3009 %}

/* FundineligOOS and FundineligI20 Student Characteristics must have a Begin Date. No enrollment validation, same as 3006/3007. */
with brule as (
    select tdoe_error_code,
        cast(error_school_year_start as int) as error_school_year_start,
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{ this.identifier }}'
),
stg_student_edorgs as (
    select seoa.k_student, seoa.k_lea, seoa.k_school, seoa.school_year, seoa.ed_org_id, seoa.student_unique_id,
        brule.tdoe_error_code, brule.tdoe_severity
    from {{ ref('stg_ef3__student_education_organization_associations') }} seoa
    join brule
        on cast(seoa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    where seoa.k_lea is not null
),
errors as (
    select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
        s.state_student_id as legacy_state_student_id,
        sc.student_characteristic, sc.begin_date,
        se.tdoe_error_code as error_code,
        concat('Missing date range for FundineligOOS / FundineligI20 student characteristic. Student ',
            se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
            'Value received: ', concat(sc.student_characteristic, ' [', coalesce(cast(sc.begin_date as string), 'null'), ' - ', coalesce(cast(sc.end_date as string), 'null'), ']')) as error,
        {{ severity_to_severity_code_case_clause('se.tdoe_severity') }},
        se.tdoe_severity
    from stg_student_edorgs se
    join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
        on se.k_student = s.k_student
    join {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
        on sc.k_lea = se.k_lea
        and sc.k_student = se.k_student
    where sc.student_characteristic in ('FundineligOOS', 'FundineligI20')
        and sc.begin_date is null
)
select *
from errors