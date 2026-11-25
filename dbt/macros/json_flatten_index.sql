{% macro json_flatten_index(alias='') %}
    {{ return(
        adapter.dispatch(
            'json_flatten_index',
            'edu_edfi_source'
        )(alias)
    ) }}
{% endmacro %}


{% macro snowflake__json_flatten_index(alias) -%}
{% if alias != '' %}{{ alias }}.{% endif %}index
{%- endmacro %}


{% macro databricks__json_flatten_index(alias) -%}
pos
{%- endmacro %}