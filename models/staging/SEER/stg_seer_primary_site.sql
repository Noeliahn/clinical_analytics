WITH stg_primary_site AS (
    SELECT * 
    FROM {{ ref('base_seer_patient')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(primary_site) AS primary_site_id
    , primary_site
        
    FROM stg_primary_site

        UNION ALL
     
    SELECT 
        MD5('no_primary_site') AS primary_site_id
        , 'no_primary_site' AS primary_site
        
    )

SELECT * FROM renamed_casted