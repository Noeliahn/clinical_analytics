WITH stg_gender AS (
    SELECT * 
    FROM {{ ref('base_seer_patient')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(gender) AS gender_id
    , gender
        
    FROM stg_gender

        UNION ALL
     
    SELECT 
        MD5('no_gender') AS gender_id
        , 'no_gender' AS gender
        
    )

SELECT * FROM renamed_casted