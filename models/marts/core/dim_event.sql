-- {{ config(materialized='table') }}

select distinct
    e.organ_system_id,
    os.organ_system,

    e.adverse_event_term_id,
    aet.adverse_event_term,

    e.ctgov_group_code_id,
    cgc.ctgov_group_code,

    e.nct_id,

    et.event_type_id,
    et.event_type,

    cl.classification_id,
    cl.classification
from {{ ref('stg_aact_reported_events') }} e
left join {{ ref('stg_aact_organ_system') }} os 
    on e.organ_system_id = os.organ_system_id
left join {{ ref('stg_aact_adverse_event_term') }} aet 
    on e.adverse_event_term_id = aet.adverse_event_term_id
left join {{ ref('stg_aact_ctgov_group') }} cgc 
    on e.ctgov_group_code_id = cgc.ctgov_group_code_id
left join {{ ref('stg_aact_event_type') }} et 
    on e.result_group_id is not null  
left join {{ ref('stg_aact_classification') }} cl 
    on e.result_group_id is not null
