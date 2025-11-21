WITH reported_events_total AS (
    SELECT * 
    FROM {{ ref('base_aact_reported_events_total') }}
),

renamed_casted AS (
    SELECT
        id_reported_events_total
        , nct_id
        , ctgov_group_code_id
        , event_type_id
        , classification_id
        , created_at
        , updated_at
    FROM reported_events_total
)

SELECT *
FROM renamed_casted
