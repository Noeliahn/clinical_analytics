WITH reported_events_total AS (
    SELECT * 
    FROM {{ source('aact', 'reported_events_total') }}
),

renamed_casted AS (
    SELECT
        id::INT                                                                     AS id_reported_events_total
        , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
        , MD5(ctgov_group_code)::VARCHAR(256)                                       AS ctgov_group_code_id
        , ctgov_group_code::VARCHAR(256)                                            AS ctgov_group_code
        , MD5(event_type)::VARCHAR(256)                                             AS event_type_id
        , event_type::VARCHAR(256)                                                  AS event_type
        , MD5(classification)::VARCHAR(256)                                         AS classification_id
        , classification::VARCHAR(256)                                              AS classification
        , CAST(created_at AS TIMESTAMP)                                             AS created_at
        , CAST(updated_at AS TIMESTAMP)                                             AS updated_at
    FROM reported_events_total
)

SELECT *
FROM renamed_casted
