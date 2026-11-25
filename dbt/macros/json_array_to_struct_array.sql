{% macro json_array_to_struct_array(array, paired_list) %}
    {{ return(
        adapter.dispatch(
            'json_array_to_struct_array',
            'edu_edfi_source'
        )(array, paired_list)
    ) }}
{% endmacro %}

{% macro databricks__json_array_to_struct_array(array, paired_list) -%}
    transform(
        cast({{ array }} as array<variant>),
        item -> named_struct(
            {%- for item in paired_list %}
                '{{ item[0] }}', {{ item[1] }}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        )
    )
{%- endmacro %}

{% macro snowflake__json_array_to_struct_array(array, paired_list) -%}
    transform(
        {{ array }},
        item -> {{ edu_edfi_source.json_object_construct(paired_list) }}
    )::array(
        object(
            {%- for item in paired_list %}
                {{ item[0] }} {{ item[2] }}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        )
    )
{%- endmacro %}