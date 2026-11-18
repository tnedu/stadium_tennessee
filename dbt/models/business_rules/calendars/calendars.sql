{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

{%- set validation_config = var('edu:data_validation', {}) -%}
{%- set severities = validation_config.get('severities', {}) -%}

{% if validation_config.get('enabled', false) %}

select cal.k_school, cal.k_school_calendar, cal.school_year, cal.school_id, cal.calendar_code,
    v.val.code as error_code,
    v.val.message as error,
    case v.val.severity
        {% for severity, rank in severities.items() %}
        when '{{ severity }}' then {{ rank }}
        {% endfor %}
    end as tdoe_severity_code,
    v.val.severity as tdoe_severity
from {{ edu_wh.validated_ref('stg_ef3__calendars') }} cal
lateral view explode(cal.validations) v as val
where v.val.severity in (
    {% for severity in severities.keys() %}
    '{{ severity }}'{% if not loop.last %},{% endif %}
    {% endfor %}
)

{% else %}

select *
from {{ ref('calendars_errors_unioned') }}
where tdoe_severity != 'potential'

{% endif %}