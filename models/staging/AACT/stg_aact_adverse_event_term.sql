WITH stg_adverse_event_term AS (
    SELECT * 
    FROM {{ ref('base_aact_reported_events')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(adverse_event_term) AS adverse_event_term_id
    , adverse_event_term
        
    FROM stg_adverse_event_term

        UNION ALL
     
    SELECT 
        MD5('no_adverse_event_term') AS adverse_event_term_id
        , 'no_adverse_event_term' AS adverse_event_term
        
    )

SELECT * FROM renamed_casted