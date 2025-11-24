WITH stg_cause_of_death AS (
    SELECT * 
    FROM {{ ref('base_seer_patient')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(cause_of_death)                 AS cause_of_death_id
    , cause_of_death
        
    FROM stg_cause_of_death

        UNION ALL
     
    SELECT 
        MD5('no_cause_of_death')        AS cause_of_death_id
        , 'no_cause_of_death'           AS cause_of_death
        
    )

SELECT * FROM renamed_casted