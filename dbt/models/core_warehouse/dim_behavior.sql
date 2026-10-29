{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_behavior set not null",
        "alter table {{ this }} add primary key (k_behavior)",
    ]
  )
}}

{{ edu_wh.cds_depends_on('edu:dim_behavior:custom_data_sources') }}
{% set custom_data_sources = var('edu:dim_behavior:custom_data_sources', []) %}

with stg_behaviors as (
    select * from {{ ref('stg_ef3__descriptors') }}
    where descriptor_name = 'behavior_descriptors'
),
tdoe_zero_tolerance_behaviors as (
    select * from {{ ref('tdoe_zero_tolerance_behaviors') }}
),
formatted as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'stg_behaviors.tenant_code',
                'stg_behaviors.school_year',
                'lower(stg_behaviors.descriptor_name)',
                'lower(stg_behaviors.namespace)',
                'lower(stg_behaviors.code_value)'
            ]
        ) }} as k_behavior,
        stg_behaviors.school_year,  
        stg_behaviors.code_value as behavior_code, 
        stg_behaviors.short_description as behavior_short_description, 
        stg_behaviors.description as behavior_description,
        case
            when zt.behavior_code_value is not null then True
            else False
        end as is_tdoe_zero_tolerance

        -- custom data sources columns
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_behaviors 
    left outer join tdoe_zero_tolerance_behaviors zt
        on zt.behavior_code_value = stg_behaviors.code_value
        and stg_behaviors.school_year between zt.zt_school_year_start and coalesce(zt.zt_school_year_end, 9999)

    -- custom data sources
    {{ edu_wh.add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_behaviors', join_cols=['k_behavior']) }}
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
order by school_year desc, behavior_code
