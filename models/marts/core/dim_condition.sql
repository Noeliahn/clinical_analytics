-- {{ config(materialized='table') }}

select distinct
    id_condition
    , nct_id
    , condition_name
    , md5(lower(trim(condition_name))) as disease_id

from {{ ref('stg_aact_conditions') }}
