WITH stg_intervention_type AS (
    SELECT * 
    FROM {{ ref('base_aact_interventions')  }}
),

renamed_casted AS (
    SELECT DISTINCT
    MD5(intervention_type) AS intervention_type_id
    , intervention_type
        
    FROM stg_intervention_type

        UNION ALL
     
    SELECT 
        MD5('no_intervention_type') AS intervention_type_id
        , 'no_intervention_type' AS intervention_type
        
    )

SELECT * 
FROM renamed_casted
