{% macro schedule_object_fields(object_name) -%}

    {%- set object_fields = var(
        'edu:schedule:object_fields',
        {}
    ) -%}

    {{ return(object_fields.get(object_name, [])) }}

{%- endmacro %}

{% macro schedule_object_select_fields(object_name, source_alias) -%}

    {%- set fields = schedule_object_fields(object_name) -%}

    {%- for field in fields %}
        {{ source_alias }}.{{ adapter.quote(field) }} as {{ adapter.quote(field) }},
    {%- endfor %}

{%- endmacro %}

{% macro schedule_object_json_fields(object_name, source_alias=none) -%}

    {%- set fields = schedule_object_fields(object_name) -%}
    {%- set json_fields = [] -%}

    {%- for field in fields -%}

        {%- if source_alias is not none -%}
            {%- set expression = source_alias ~ '.' ~ adapter.quote(field) -%}
        {%- else -%}
            {%- set expression = adapter.quote(field) -%}
        {%- endif -%}

        {%- do json_fields.append([
            field,
            expression
        ]) -%}

    {%- endfor -%}

    {{ return(json_fields) }}

{%- endmacro %}

{% macro schedule_object_group_by_fields(object_name, source_alias=none) -%}

    {%- set fields = schedule_object_fields(object_name) -%}

    {%- for field in fields %}
        ,
        {%- if source_alias is not none %}
        {{ source_alias }}.{{ adapter.quote(field) }}
        {%- else %}
        {{ adapter.quote(field) }}
        {%- endif %}
    {%- endfor %}

{%- endmacro %}
