WITH drop_withdrawals AS (
    SELECT * 
    FROM {{ source('aact', 'drop_withdrawals')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_drop_withdrawals
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id
    , MD5(result_group_id)::INT                                                 AS result_group_id
    , result_group_id::INT                                                    AS result_group_name
    , MD5(ctgov_group_code)::VARCHAR(256)                                       AS ctgov_group_code_id
    , ctgov_group_code::VARCHAR(256)                                            AS ctgov_group_code
    , MD5(period)::VARCHAR(256)                                                 AS period_id
    , period::VARCHAR(256)                                                      AS period
    , MD5(reason)::VARCHAR(256)                                                 AS reason_id
    , reason::VARCHAR(256)                                                      AS reason
    , count::INT                                                                AS count

    FROM drop_withdrawals
)

SELECT * 
FROM renamed_casted


