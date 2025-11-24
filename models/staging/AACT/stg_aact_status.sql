WITH stg_status AS (
    SELECT * 
    FROM {{ ref('base_aact_facilities')  }}
),

renamed_casted AS (
    SELECT DISTINCT
    MD5(status) AS status_id
    , status
        
    FROM stg_status

        UNION ALL
     
    SELECT 
        MD5('no_status') AS status_id
        , 'no_status' AS status
        
    )

SELECT * FROM renamed_casted