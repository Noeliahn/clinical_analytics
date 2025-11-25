



-- {{ config(materialized='table') }}

with base as (
    select distinct
        nct_id
    from {{ ref('stg_aact_design_group') }}
),

condition_count as (
    select
        nct_id,
        count(*) as condition_count
    from {{ ref('stg_aact_conditions') }}
    group by 1
),

facility_count as (
    select
        nct_id,
        count(*) as facility_count
    from {{ ref('stg_aact_facilities') }}
    group by 1
),

intervention_count as (
    select
        nct_id,
        count(*) as intervention_count
    from {{ ref('stg_aact_interventions') }}
    group by 1
)

select
    b.nct_id,
    coalesce(c.condition_count, 0)      as condition_count,
    coalesce(f.facility_count, 0)       as facility_count,
    coalesce(i.intervention_count, 0)   as intervention_count,
    current_timestamp                   as ingestion_date
from base b
left join condition_count c on b.nct_id = c.nct_id
left join facility_count f on b.nct_id = f.nct_id
left join intervention_count i on b.nct_id = i.nct_id




-- base_aact_study → título y fecha de carga

-- base_aact_conditions → número de condiciones asociadas

-- base_aact_facilities → número de hospitales/centros

-- base_aact_interventions → número de intervenciones del estudio