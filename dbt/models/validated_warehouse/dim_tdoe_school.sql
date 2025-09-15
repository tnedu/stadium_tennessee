-- depends_on: {{ ref('dim_network') }}
{{
  config(
    materialized="table",
    alias='dim_school',
    schema="tdoe_wh",
    post_hook=[
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} add primary key (k_school)",

        "{% set network_types = dbt_utils.get_column_values(
                           table=ref('xwalk_network_association_types'),
                           column='network_type',
                           where=\"association_type = 'school'\",
                           order_by='network_type')
        %}
        {% if network_types is not none %}
        {% for network_type in network_types %} 
            alter table {{ this }} add constraint fk_{{this.name}}_{{network_type}}_network foreign key (k_network__{{network_type}}) references {{ ref('dim_tdoe_network') }} (k_network)
            {%if not loop.last%};{%endif%}
        {% endfor %}
        {% endif %}"
    ]
  )
}}

{{ 
  append_status_columns(
    source_table='dim_school',
    status_table=None,
    status_table_join_columns=None
  ) 
}}