{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

select * from {{ ref('bld_ilpd_safe_ranges_program_services') }}
union
select * from {{ ref('bld_ilpd_safe_ranges_participation_statuses') }}