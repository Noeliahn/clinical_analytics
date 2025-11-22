WITH reported_events AS (
    SELECT * 
    FROM {{ ref('base_aact_reported_events') }}
),

renamed_casted AS (
    SELECT
        id_reported_events
        , nct_id
        , result_group_id
        , ctgov_group_code_id
        , subjects_affected
        , subjects_at_risk
        , event_count
        , organ_system_id
        , adverse_event_term_id

    FROM reported_events
)

SELECT *
FROM renamed_casted
