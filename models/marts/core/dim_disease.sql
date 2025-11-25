-- {{ config(materialized='table') }}
with aact_conditions as (
    select distinct
        lower(trim(condition_name)) as disease_name,
        'aact' as source
    from {{ ref('stg_aact_conditions') }}
),

seer_primary_sites as (
    select distinct
        lower(trim(primary_site)) as disease_name,
        'seer' as source
    from {{ ref('stg_seer_primary_site') }}
),

unioned as (
    select * from aact_conditions
    union all
    select * from seer_primary_sites
),

aggregated as (
    select
        disease_name,
        md5(disease_name) as disease_id,
        array_agg(distinct source) as sources
    from unioned
    group by 1
)

select
    disease_id,
    disease_name as disease_name_normalized,
    sources,
    current_timestamp as ingestion_date
from aggregated
