WITH drop_withdrawals AS (
    SELECT * 
    FROM {{ ref('base_aact_drop_withdrawals')  }}
),

renamed_casted AS (
    SELECT
    id_drop_withdrawals
    , nct_id
    , result_group_id
    , ctgov_group_code_id
    , period_id
    , reason_id
    , count

    FROM drop_withdrawals
)

SELECT * 
FROM renamed_casted

