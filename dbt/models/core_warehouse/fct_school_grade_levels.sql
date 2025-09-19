{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column grade_level set not null",
        "alter table {{ this }} add primary key (k_school, grade_level)"
    ]
  )
}}

{{ edu_wh.cds_depends_on('tdoe:school_grade_levels:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:school_grade_levels:custom_data_sources', []) %}

/* TODO: This doesn't work for school grade levels by school year yet. */
with stg_school_grade_levels as (
    select * from {{ ref('stg_ef3__schools__grade_levels') }}
),
formatted as (
    select sgl.tenant_code, sgl.k_school, sgl.grade_level,
        x.grade_level_short, x.grade_level_integer

        -- custom indicators
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_school_grade_levels sgl
    join {{ ref('xwalk_grade_levels') }} x
        on upper(x.grade_level) = upper(sgl.grade_level)
      
    -- custom data sources
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
order by tenant_code, k_school