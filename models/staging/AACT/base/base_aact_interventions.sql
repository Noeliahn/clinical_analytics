WITH interventions AS (
    SELECT * 
    FROM {{ source('aact', 'interventions')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_interventions
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
    , (CASE 
        WHEN intervention_type IS NULL THEN MD5('no_intervention_type' )
        ELSE MD5(intervention_type)
    END)::VARCHAR(256)                                                          AS intervention_type_id
    , intervention_type::VARCHAR(256)                                           AS intervention_type
    , name::VARCHAR(256)                                                        AS name
    , description::VARCHAR(1024)                                                AS description
    
    FROM interventions
)

SELECT * 
FROM renamed_casted
