WITH conditions AS (
    SELECT * 
    FROM {{ source('aact', 'conditions')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_condition
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
    , downcase_name::VARCHAR(256)                                               AS condition_name
    
    FROM conditions
)

SELECT * 
FROM renamed_casted
