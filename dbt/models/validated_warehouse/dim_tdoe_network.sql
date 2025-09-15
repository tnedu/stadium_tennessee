{{
  config(
    materialized="table",
    alias='dim_network',
    schema="tdoe_wh",
    post_hook=[
        "alter table {{ this }} alter column k_network set not null",
        "alter table {{ this }} add primary key (k_network)",
    ]
  )
}}

{{ 
  append_status_columns(
    source_table='dim_network',
    status_table=None,
    status_table_join_columns=None
  ) 
}}