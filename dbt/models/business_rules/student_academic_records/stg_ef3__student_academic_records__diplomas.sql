{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records_orig') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        {{ edu_edfi_source.extract_descriptor('value:diplomaTypeDescriptor::string') }} as diploma_type,
        value:diplomaAwardDate::date as diploma_award_date,
        value:diplomaDescription::string as diploma_description,
        {{ edu_edfi_source.extract_descriptor('value:diplomaLevelDescriptor::string') }} as diploma_level_descriptor,
        {{ edu_edfi_source.extract_descriptor('value:achievementCategoryDescriptor::string') }} as achievement_category_descriptor,
        value:achievementCategorySystem::string as achievement_category_system,
        value:achievementTitle::string as achievement_title,
        value:criteria::string as criteria,
        value:criteriaUrl::string as criteria_url,
        value:cteCompleter::boolean as is_cte_completer,
        value:diplomaAwardExpiresDate::date as diploma_award_expires_date,
        value:evidenceStatement::string as evidence_statement,
        value:imageURL::string as image_url,
        value:issuerName::string as issuer_name,
        value:issuerOriginURL::string as issuer_origin_url,
        -- edfi extensions
        value:_ext as v_ext 
    from stg_academic_records
        , lateral variant_explode(v_diplomas)
),
-- pull out extensions from v_diplomas.v_ext to their own columns
extended as (
    select 
        flattened.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}

    from flattened
)
select * from extended
