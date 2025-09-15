{{
  config(
    materialized="table",
    alias='dim_school_calendar',
    schema="tdoe_wh",
    post_hook=[
        "alter table {{ this }} alter column k_school_calendar set not null",
        "alter table {{ this }} add primary key (k_school_calendar)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_tdoe_school') }}",
    ]
  )
}}

with bad as (
    select k_school_calendar, 
        max(tdoe_severity_code) as tdoe_severity_code,
        {{ severity_code_to_severity_case_clause('max(tdoe_severity_code)') }}
    from {{ ref('calendars') }}
    group by k_school_calendar
)
{{ 
  append_status_columns(
    source_table='dim_school_calendar',
    status_table='bad',
    status_table_join_columns=['k_school_calendar']
  ) 
}}