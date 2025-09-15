{%- macro get_school_from_school_id(school_id) -%}
    cast(right(right(concat('00000000', {{ school_id }}), 8), 4) as int)
{%- endmacro -%}