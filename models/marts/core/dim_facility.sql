-- {{ config(materialized='table') }}

select distinct
    id_facilities,
    nct_id,
    status_id,
    country_id,
    country,
    latitude,
    longitude
from {{ ref('stg_aact_facilities') }}
