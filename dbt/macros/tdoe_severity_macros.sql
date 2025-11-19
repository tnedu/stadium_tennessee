{%- macro severity_to_severity_code_case_clause(severity) -%}
    {%- set status_columns_var = var("tdoe:status_columns_config", none) -%}

    {%- set when_clauses = [] -%}
    {%- for status_name, status_code in status_columns_var['statuses'].items() -%}
        {%- set when_clause = "when '" ~ status_name ~ "' then " ~ status_code -%}
        {%- do when_clauses.append(when_clause) -%}
    {%- endfor %}
    case {{ severity }}
        {{ when_clauses | join('\n') }}
    end as tdoe_severity_code
{%- endmacro -%}

{%- macro severity_code_to_severity_case_clause(severity_code) -%}
    {%- set status_columns_var = var("tdoe:status_columns_config", none) -%}

    {%- set when_clauses = [] -%}
    {%- for status_name, status_code in status_columns_var['statuses'].items() -%}
        {%- set when_clause = "when " ~ status_code ~ " then '" ~ status_name ~ "'" -%}
        {%- do when_clauses.append(when_clause) -%}
    {%- endfor %}
    case {{ severity_code }}
        {{ when_clauses | join('\n') }}
    end as tdoe_severity
{%- endmacro -%}

{%- macro append_status_columns(source_table, status_table, status_table_join_columns) -%}
    {%- set status_columns_var = var("tdoe:status_columns_config", none) -%}

    {%- if status_table is none -%}
        {%- set status_cols = [] -%}
        {%- for col_name, col_value in status_columns_var['columns'].items() -%}
            {%- set col = col_value ~ " as " ~ col_name -%}
            {%- do status_cols.append(col) -%}
        {%- endfor %}
        select x.*,
            {{ status_cols | join(', ') }}
        from {{ ref(source_table) }} x
    {% else -%}
        {%- set status_cols = [] -%}
        {%- for col_name, col_value in status_columns_var['columns'].items() -%}
            {%- set col = "coalesce(" ~ status_table ~ "." ~ col_name ~ ", " ~ col_value ~ ") as " ~ col_name -%}
            {%- do status_cols.append(col) -%}
        {%- endfor %}
        {%- set join_cols = [] -%}
        {%- for col_name in status_table_join_columns -%}
            {%- set col = status_table ~ "." ~ col_name ~ " = " ~ "x." ~ col_name -%}
            {%- do join_cols.append(col) -%}
        {%- endfor %}
        select x.*,
            {{ status_cols | join(', ') }}
        from {{ ref(source_table) }} x
        left outer join {{ status_table }} 
            on {{ join_cols | join('and ') }}
    {%- endif -%}
{%- endmacro -%}