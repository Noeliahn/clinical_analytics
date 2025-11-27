WITH stg_facilities AS (
    SELECT * 
    FROM {{ ref('base_aact_facilities')  }}
),

renamed_casted AS (
    SELECT
    id_facilities
    , nct_id
    , status_id
    , name
    , country_id
    , country
    , latitude
    , longitude

    FROM stg_facilities
)

SELECT * 
FROM renamed_casted
