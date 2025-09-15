{%- macro gen_school_year_unions(school_year_ranges=none) -%}
    {%- if school_year_ranges is none -%}
        {%- set school_year_ranges = [{'begin_year': 2018, 'end_year': none}] -%}
    {%- endif -%}
    
    {%- set current_month = modules.datetime.datetime.now().month -%}
    {%- set current_school_year = modules.datetime.datetime.now().year -%}
    {%- if 7 <= current_month <= 12 -%}
        {%- set current_school_year = current_school_year + 1 -%}
    {%- endif -%}

    {%- set sql_output = [] -%}
    {%- for record in school_year_ranges -%}
        {%- set end_year = record.end_year if record.end_year is not none else current_school_year -%}
        {%- for year in range(record.begin_year | int, end_year + 1 ) -%}
            {% do sql_output.append("select " ~ year ~ " as school_year") -%}
        {%- endfor -%}
    {%- endfor -%}
    {{ sql_output | join(" union\n    ") }}
{%- endmacro -%}