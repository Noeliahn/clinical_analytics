WITH stg_result_group AS (
    SELECT * 
    FROM {{ ref('base_aact_drop_withdrawals')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(result_group_id) AS result_group_id
    , result_group_name
        
    FROM stg_result_group

        UNION ALL
     
    SELECT 
        MD5('no_result_group') AS result_group_id
        , 'no_result_group' AS result_group_name
        
    )

SELECT * FROM renamed_casted