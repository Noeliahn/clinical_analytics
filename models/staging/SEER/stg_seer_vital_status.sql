WITH stg_vital_status AS (
    SELECT * 
    FROM {{ ref('base_seer_patient')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(vital_status) AS vital_status_id
    , vital_status
        
    FROM stg_vital_status

        UNION ALL
     
    SELECT 
        MD5('no_vital_status') AS vital_status_id
        , 'no_vital_status' AS vital_status
        
    )

SELECT * FROM renamed_casted