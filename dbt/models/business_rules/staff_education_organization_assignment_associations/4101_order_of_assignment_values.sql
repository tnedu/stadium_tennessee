{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4101 %}

with stg_staff_edorg_assignment_assoc as (
    select * from {{ ref('stg_ef3__staff_education_organization_assignment_associations') }} seoas
    where 1=1
        {{ school_year_exists(error_code, 'seoas') }}
),
errors as (
    /* Order of Assignment can be null or 1, 2, or 3 only. */
    select seaa.k_staff, seaa.k_lea, seaa.k_school, seaa.school_year, seaa.ed_org_id, seaa.staff_unique_id,
        seaa.begin_date, seaa.staff_classification,
        {{ error_code }} as error_code,
        concat('Order of Assignment can be [null], 1, 2, or 3. Value Received: ', 
            seaa.order_of_assignment, '.',
            ' Staff Email: ', coalesce(s.email_address, '[no value]')) as error
    from stg_staff_edorg_assignment_assoc seaa
    join {{ ref('dim_staff')}} s
        on s.k_staff = seaa.k_staff
    where seaa.order_of_assignment is not null
        and seaa.order_of_assignment not in (1, 2, 3)
)
select errors.*,
    {{ severity_to_severity_code_case_clause('rules.tdoe_severity') }},
    rules.tdoe_severity
from errors errors
join {{ ref('business_rules_year_ranges') }} rules
    on rules.tdoe_error_code = {{ error_code }}