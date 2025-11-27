-- {{ config(materialized='table') }}
-- fct_facility_enrollment
select
    id_facilities,
    nct_id,
    status_id,
    country_id
from {{ ref('stg_aact_facilities') }}

