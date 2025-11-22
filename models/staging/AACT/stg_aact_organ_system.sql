WITH stg_organ_system AS (
    SELECT * 
    FROM {{ ref('base_aact_reported_events')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(organ_system) AS organ_system_id
    , organ_system
        
    FROM stg_organ_system

        UNION ALL
     
    SELECT 
        MD5('no_organ_system') AS organ_system_id
        , 'no_organ_system' AS organ_system
        
    )

SELECT * FROM renamed_casted