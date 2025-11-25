WITH sponsors AS (
    SELECT * 
    FROM {{ ref('base_aact_sponsors') }}
),

renamed_casted AS (
    SELECT
        id_reported_events
        , nct_id
        , agency_class_id
        , agency_class
        , lead_or_collaborator_id
        , lead_or_collaborator
        , sponsor_name                                                


    FROM sponsors
)

SELECT *
FROM renamed_casted

