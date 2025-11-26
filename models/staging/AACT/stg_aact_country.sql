WITH stg_country AS (
    SELECT * 
    FROM {{ ref('base_aact_facilities')  }}
),

renamed_casted AS (
    SELECT DISTINCT
    MD5(country) AS country_id
    , country

        
    FROM stg_country

        UNION ALL
     
    SELECT 
        MD5('no_country') AS country_id
        , 'no_country' AS country
        
    )

SELECT * FROM renamed_casted