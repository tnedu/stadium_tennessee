{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4300 %}

/* The purpose of this business rule is to validate that a Staff's email address domain name is white
   listed for that District. This uses a foreign catalog in Databricks containing a table of 
   white listed domain names per District. */

with brule as (
    /* Fetch the business rule criteria for this model */
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ source('stadium_tennessee', 'business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
        and rule_model = '{{this.identifier}}'
),
stg_staff_edorg_contact_assoc as (
    /* Get all Staff EdOrg Contact Assoc that should be checked for this rule. 
       Create helper columns:
         - district_number: a column to use as the District Number because the 
           ed_org_id could be a District or School, and if it's a School, we 
           need that School's District Number.
         - email_domain: the domain name of the email address that we will use
           to compare against. */
    select seoca.*,
        brule.tdoe_error_code, brule.tdoe_severity,
        case
            when seoca.ed_org_id < 999 then ed_org_id
            else cast(left(right(concat('00000000', seoca.ed_org_id), 8), 4) as int)
        end as district_number,
        nullif(
            regexp_extract(trim(seoca.email_address), r'@([^@\s]+)$', 1),
            ''
        ) as email_domain
    from {{ ref('stg_ef3__staff_education_organization_contact_associations') }} seoca
    join brule brule
        on seoca.school_year between brule.error_school_year_start and brule.error_school_year_end
),
white_listed_domains as (
    /* Fetch the active white listed domain names. */
    select *
    from {{ source('identity', 'whitelisteddomains') }}
    where Active is true
),
bad_domains as (
    /* Identify email addresses with non-white-listed domain names. */
    select *
    from stg_staff_edorg_contact_assoc seoca
    where not exists (
        select 1
        from white_listed_domains wl
        where wl.DistrictNumber = seoca.district_number
            and upper(wl.EmailDomain) = upper(seoca.email_domain)
    )
),
errors as (
    /* Generate the errors to be displayed. */
    select k_staff, k_lea, k_school, school_year, tenant_code, api_year, contact_title, ed_org_id, 
        email_address, staff_unique_id, 
        tdoe_error_code,
        concat('Staff ', staff_unique_id, ' at ',
            (
                case 
                    when ed_org_id != district_number then concat('School ', ed_org_id)
                    else concat('District ', ed_org_id)
                end
            ),
            ' has an email domain that is not approved for this District. Value Received: ',
            'District: ', district_number, ', ',
            'Contact Title: ', contact_title, ', ',
            'Email: ', email_address
        ) as error,
        {{ severity_to_severity_code_case_clause('tdoe_severity') }},
        tdoe_severity
    from bad_domains
)
select *
from errors