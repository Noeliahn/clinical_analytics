WITH stg_lead_or_collaborator AS (
    SELECT * 
    FROM {{ ref('base_aact_sponsors')  }}
),

renamed_casted AS (
    SELECT DISTINCT
    MD5(lead_or_collaborator) AS lead_or_collaborator_id
    , lead_or_collaborator
        
    FROM stg_lead_or_collaborator

        UNION ALL
     
    SELECT 
        MD5('no_lead_or_collaborator') AS lead_or_collaborator_id
        , 'no_lead_or_collaborator' AS lead_or_collaborator
        
    )

SELECT * FROM renamed_casted