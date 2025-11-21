WITH stg_reason AS (
    SELECT * 
    FROM {{ ref('base_aact_drop_withdrawals')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(reason) AS reason_id
    , reason
        
    FROM stg_reason

        UNION ALL
     
    SELECT 
        MD5('no_reason') AS reason_id
        , 'no_reason' AS reason
        
    )

SELECT * FROM renamed_casted