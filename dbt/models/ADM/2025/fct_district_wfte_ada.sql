{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} alter column report_period set not null",
        "alter table {{ this }} alter column fteada_program set not null",
        "alter table {{ this }} add primary key (k_lea, report_period, fteada_program)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('edu_wh', 'dim_lea') }}"
    ]
  )
}}

select fteada.k_lea,
    fteada.report_period, 
    fteada.fteada_program,
    cast((floor(sum(fteada.normalized_fteada) * cast(fteada.fteada_weight as decimal(8,5)) * 100000) / 100000) as decimal(12,5)) as wfteada
from {{ ref('fct_student_fte_ada') }} fteada
where fteada.is_primary_school = true
    and fteada.fteada_program is not null
group by fteada.k_lea,
    fteada.report_period, 
    fteada.fteada_program, fteada.fteada_weight