{% macro to_military_time(expression) -%}
case
    when {{ expression }} is null then null
    else concat(
        lpad(
            cast(
                case
                    when split_part({{ expression }}, ':', 1)::int between 1 and 6
                    then split_part({{ expression }}, ':', 1)::int + 12
                    else split_part({{ expression }}, ':', 1)::int
                end
                as string
            ),
            2,
            '0'
        ),
        ':',
        lpad(split_part({{ expression }}, ':', 2), 2, '0'),
        ':',
        lpad(
            coalesce(
                nullif(split_part({{ expression }}, ':', 3), ''),
                '00'
            ),
            2,
            '0'
        )
    )
end
{%- endmacro %}