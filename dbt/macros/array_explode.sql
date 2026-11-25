{% macro array_explode(column, alias='exploded_array', outer=False) %}
    {{ return(
        adapter.dispatch('array_explode', 'edu_edfi_source')(column, alias, outer)
    ) }}
{% endmacro %}


{% macro databricks__array_explode(column, alias, outer) -%}
, lateral explode{% if outer %}_outer{% endif %}({{ column }}) as {{ alias }}
{%- endmacro %}


{% macro snowflake__array_explode(column, alias, outer) -%}
, lateral flatten(
    input => {{ column }},
    outer => {{ outer }}
) as {{ alias }}
{%- endmacro %}

{% macro array_explode_value(alias='exploded_array') %}
    {{ return(
        adapter.dispatch('array_explode_value', 'edu_edfi_source')(alias)
    ) }}
{% endmacro %}


{% macro databricks__array_explode_value(alias) -%}
{{ alias }}.col
{%- endmacro %}


{% macro snowflake__array_explode_value(alias) -%}
{{ alias }}.value
{%- endmacro %}