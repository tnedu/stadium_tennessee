-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} add primary key (k_course_section, k_class_period)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_class_period foreign key (k_class_period) references {{ ref('dim_class_period') }}"
    ]
  )
}}
  
with sections as (
    select * from {{ ref('dim_course_section') }}
),
section_class_periods as (
    select * from {{ ref('stg_ef3__sections__class_periods') }}
),
class_periods as (
    select * from {{ ref('dim_class_period') }}
),
joined as (
    select 
        section_class_periods.k_course_section,
        section_class_periods.k_class_period,
        section_class_periods.tenant_code,
        sections.school_year
    from section_class_periods
    join sections
        on section_class_periods.k_course_section = sections.k_course_section
    join class_periods 
        on section_class_periods.k_class_period = class_periods.k_class_period
)
select * from joined
