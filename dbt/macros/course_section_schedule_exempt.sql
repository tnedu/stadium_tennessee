{% macro course_section_is_schedule_exempt() %}
    {%- set conditions = var(
        'edu:section:schedule_exempt_conditions',
        []
    ) -%}

    {%- if conditions | length == 0 -%}
        false
    {%- else -%}
        (
            {%- for condition in conditions %}
                coalesce(
                    {{ condition }},
                    false
                )
                {%- if not loop.last %}
                    or
                {%- endif %}
            {%- endfor %}
        )
    {%- endif -%}
{% endmacro %}