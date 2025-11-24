WITH stg_group_type AS (
    SELECT * 
    FROM {{ ref('base_aact_design_group')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(group_type) AS group_type_id
    , group_type
        
    FROM stg_group_type

        UNION ALL
     
    SELECT 
        MD5('no_group_type') AS group_type_id
        , 'no_group_type' AS group_type
        
    )

SELECT * FROM renamed_casted