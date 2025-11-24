WITH stg_ctgov_group_code AS (
    SELECT * 
    FROM {{ ref('base_aact_drop_withdrawals')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(ctgov_group_code) AS ctgov_group_code_id
    , ctgov_group_code
        
    FROM stg_ctgov_group_code

        UNION ALL
     
    SELECT 
        MD5('no_ctgov_group_code') AS ctgov_group_code_id
        , 'no_ctgov_group_code' AS ctgov_group_code
        
    )

SELECT * FROM renamed_casted