WITH design_group_interventions AS (
    SELECT * 
    FROM {{ source('aact', 'design_group_interventions')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_design_group_interventions
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
    , design_group_id::INT                                                      AS design_group_id
    , intervention_id::INT                                                      AS intervention_id
    
    FROM design_group_interventions
)

SELECT * 
FROM renamed_casted
