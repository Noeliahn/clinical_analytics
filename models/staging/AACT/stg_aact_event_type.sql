WITH stg_event_type AS (
    SELECT * 
    FROM {{ ref('base_aact_reported_events_total')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(event_type) AS event_type_id
    , event_type
        
    FROM stg_event_type

        UNION ALL
     
    SELECT 
        MD5('no_event_type') AS event_type_id
        , 'no_event_type' AS event_type
        
    )

SELECT * FROM renamed_casted