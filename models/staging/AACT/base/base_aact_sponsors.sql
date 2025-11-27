WITH sponsors AS (
    SELECT * 
    FROM {{ source('aact', 'sponsors') }}
),

renamed_casted AS (
    SELECT
        id::INT                                                                     AS id_reported_events
        , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id

    , (CASE 
        WHEN agency_class IS NULL THEN MD5('no_agency_class' )
        ELSE MD5(agency_class)
    END)::VARCHAR(256)                                                              AS agency_class_id
        , agency_class::VARCHAR(256)                                                AS agency_class
        
    , (CASE 
        WHEN lead_or_collaborator IS NULL THEN MD5('no_lead_or_collaborator' )
        ELSE MD5(lead_or_collaborator)
    END)::VARCHAR(256)                                                              AS lead_or_collaborator_id
        , lead_or_collaborator::VARCHAR(256)                                        AS lead_or_collaborator
        , name::VARCHAR(256)                                                        AS sponsor_name


    FROM sponsors
)

SELECT *
FROM renamed_casted

