WITH design_groups AS (
    SELECT * 
    FROM {{ source('aact', 'design_groups')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_design_groups
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
    , MD5(group_type)::VARCHAR(256)                                             AS group_type_id
    , group_type::VARCHAR(256)                                                  AS group_type
    , title::VARCHAR(256)                                                       AS title
    , description::VARCHAR(1024)                                                AS description
    
    FROM design_groups
)

SELECT * 
FROM renamed_casted
