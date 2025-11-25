-- {{ config(materialized='table') }}

select
    id_facilities,
    nct_id,
    status_id,
    country_id
from {{ ref('stg_aact_facilities') }}
