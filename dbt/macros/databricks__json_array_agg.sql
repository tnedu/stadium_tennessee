{% macro databricks__json_array_agg(expression, window, order_by, is_terminal) -%}

    {%- if is_terminal -%}
    parse_json(to_json(
    {%- endif -%}

    {%- if order_by is not none -%}

    transform(
        array_sort(
            array_agg(
                named_struct(
                    '__sort_key',
                    struct({{ order_by }}),
                    '__value',
                    {{ expression }}
                )
            )
            {{ window }}
        ),
        x -> x.__value
    )

    {%- else -%}

    array_agg({{ expression }})
    {{ window }}

    {%- endif -%}

    {%- if is_terminal -%}
    ))
    {%- endif -%}

{%- endmacro %}