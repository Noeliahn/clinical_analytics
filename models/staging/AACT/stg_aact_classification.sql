WITH stg_classification AS (
    SELECT * 
    FROM {{ ref('base_aact_reported_events_total')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(classification) AS classification_id
    , classification
        
    FROM stg_classification

        UNION ALL
     
    SELECT 
        MD5('no_classification') AS classification_id
        , 'no_classification' AS classification
        
    )

SELECT * FROM renamed_casted