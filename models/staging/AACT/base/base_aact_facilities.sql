WITH facilities AS (
    SELECT * 
    FROM {{ source('aact', 'facilities')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_facilities
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
    , MD5(status)::VARCHAR(256)                                                 AS status_id
    , status::VARCHAR(256)                                                      AS status
    , name::VARCHAR(256)                                                        AS name
    , MD5(country)::VARCHAR(256)                                                AS country_id
    , country::VARCHAR(256)                                                     AS country
    , latitude                                                                  AS latitude
    , longitude                                                                 AS longitude

    FROM facilities
)

SELECT * 
FROM renamed_casted
