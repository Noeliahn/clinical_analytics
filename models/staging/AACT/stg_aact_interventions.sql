WITH interventions AS (
    SELECT * 
    FROM {{ ref('base_aact_interventions')  }}
),

renamed_casted AS (
    SELECT
    id_interventions
    , nct_id
    , intervention_type_id
    , name
    , description

    
    FROM interventions
)

SELECT * 
FROM renamed_casted
