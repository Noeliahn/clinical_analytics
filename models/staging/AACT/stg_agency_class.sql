WITH stg_agency_class AS (
    SELECT * 
    FROM {{ ref('base_aact_sponsors')  }}
),

renamed_casted AS (
    SELECT DISTINCT
    MD5(agency_class) AS agency_class_id
    , agency_class
        
    FROM stg_agency_class

        UNION ALL
     
    SELECT 
        MD5('no_agency_class') AS agency_class_id
        , 'no_agency_class' AS agency_class
        
    )

SELECT * FROM renamed_casted