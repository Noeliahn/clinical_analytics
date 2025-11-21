WITH stg_period AS (
    SELECT * 
    FROM {{ ref('base_aact_drop_withdrawals')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(period) AS period_id
    , period
        
    FROM stg_period

        UNION ALL
     
    SELECT 
        MD5('no_period') AS period_id
        , 'no_period' AS period
        
    )

SELECT * FROM renamed_casted