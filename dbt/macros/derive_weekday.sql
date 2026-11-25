{% macro day_of_week(expression) %}
    {{ return(
        adapter.dispatch(
            'day_of_week',
            'edu_edfi_source'
        )(expression)
    ) }}
{% endmacro %}


{% macro databricks__day_of_week(expression) -%}
    extract(dayofweek_iso from {{ expression }})
{%- endmacro %}


{% macro snowflake__day_of_week(expression) -%}
    dayofweekiso({{ expression }})
{%- endmacro %}
