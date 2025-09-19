{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} alter column student_characteristic set not null",
        "alter table {{ this }} alter column begin_date set not null",
        "alter table {{ this }} add primary key (k_student, k_lea, student_characteristic, begin_date)"
    ]
  )
}}

{{ edu_wh.cds_depends_on('tdoe:student_characteristics:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:student_characteristics:custom_data_sources', []) %}

select c.tenant_code, c.k_student, c.k_student_xyear, c.ed_org_id, c.k_lea,
    c.student_characteristic, c.begin_date, c.end_date
    -- custom indicators
    {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
from {{ ref('stg_ef3__stu_ed_org__characteristics') }} c
-- custom data sources
{{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
where c.k_lea is not null
    and c.student_characteristic is not null
    and c.begin_date is not null
    and not exists (
        select 1
        from {{ ref('xwalk_student_characteristics') }} x
        where upper(x.characteristic_descriptor) = upper(c.student_characteristic)
    )
    and exists (
        /* The student must be in dim_student after the business rules are applied. */
        select 1
        from {{ ref('dim_student') }} x
            where x.tenant_code = c.tenant_code
            and x.k_student = c.k_student
    )