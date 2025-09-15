{%- macro get_district_from_school_id(school_id) -%}
    cast(left(right(concat('00000000', {{ school_id }}), 8), 4) as int)
{%- endmacro -%}