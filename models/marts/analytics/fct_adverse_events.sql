-- {{ config(materialized='incremental', unique_key='id_reported_events') }}

select
    id_reported_events,
    nct_id,
    organ_system_id,
    adverse_event_term_id,
    ctgov_group_code_id,
    subjects_affected,
    subjects_at_risk,
    event_count
from {{ ref('stg_aact_reported_events') }}

-- {% if is_incremental() %}
-- where updated_at > (select max(updated_at) from {{ this }})
-- {% endif %};
