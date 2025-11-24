WITH reported_events AS (
    SELECT * 
    FROM {{ source('aact', 'reported_events') }}
),

renamed_casted AS (
    SELECT
        id::INT                                                                     AS id_reported_events
        , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
        , result_group_id::INT                                                      AS result_group_id

    , (CASE 
        WHEN ctgov_group_code IS NULL THEN MD5('no_ctgov_group_code' )
        ELSE MD5(ctgov_group_code)
    END)::VARCHAR(256)                                                              AS ctgov_group_code_id
        , ctgov_group_code::VARCHAR(256)                                            AS ctgov_group_code

        , subjects_affected::INT                                                    AS subjects_affected
        , subjects_at_risk::INT                                                     AS subjects_at_risk
        , event_count::INT                                                          AS event_count

    , (CASE 
        WHEN organ_system IS NULL THEN MD5('no_organ_system' )
        ELSE MD5(organ_system)
    END)::VARCHAR(256)                                                              AS organ_system_id
        , organ_system::VARCHAR(256)                                                AS organ_system


    , (CASE 
        WHEN adverse_event_term IS NULL THEN MD5('no_adverse_event_term' )
        ELSE MD5(adverse_event_term)
    END)::VARCHAR(256)                                                              AS adverse_event_term_id
        , adverse_event_term::VARCHAR(256)                                          AS adverse_event_term

    FROM reported_events
)

SELECT *
FROM renamed_casted

