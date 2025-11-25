-- {{ config(materialized='table') }}

select distinct
    id_interventions
    , nct_id
    , intervention_type_id
    , name as intervention_name
    , description
from {{ ref('stg_aact_interventions') }}
