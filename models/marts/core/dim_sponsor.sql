-- {{ config(materialized='table') }}

select distinct
    s.id_reported_events as sponsor_row_id,
    s.nct_id,
    s.agency_class_id,
    ac.agency_class,
    s.lead_or_collaborator_id,
    lc.lead_or_collaborator,
    s.name as sponsor_name
from {{ ref('stg_aact_sponsors') }} s
left join {{ ref('stg_aact_agency_class') }} ac 
    on s.agency_class_id = ac.agency_class_id
left join {{ ref('stg_aact_lead_or_collaborator') }} lc
    on s.lead_or_collaborator_id = lc.lead_or_collaborator_id;
