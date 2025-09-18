{{
  config(
    materialized="table",
    schema="cds"
  )
}}

{# Load status column config #}
{% set status_columns_var = var("tdoe:status_columns_config", none) %}

{%- set status_cols = [] -%}
{%- for col_name, col_value in status_columns_var['columns'].items() -%}
    {%- set col = col_value ~ " as " ~ col_name -%}
    {%- do status_cols.append(col) -%}
{%- endfor %}
select 
    {{ status_cols | join(', ') }}