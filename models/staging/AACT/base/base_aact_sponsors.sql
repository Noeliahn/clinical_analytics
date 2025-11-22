WITH sponsors AS (
    SELECT * 
    FROM {{ source('aact', 'sponsors') }}
),

renamed_casted AS (
    SELECT
        id::INT                                                                     AS id_reported_events
        , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
        , MD5(agency_class)::VARCHAR(256)                                           AS agency_class_id
        , agency_class::VARCHAR(256)                                                AS agency_class
        , MD5(lead_or_collaborator)::VARCHAR(256)                                   AS lead_or_collaborator_id
        , lead_or_collaborator::VARCHAR(256)                                        AS lead_or_collaborator
        , name::VARCHAR(256)                                                        AS name


    FROM sponsors
)

SELECT *
FROM renamed_casted

