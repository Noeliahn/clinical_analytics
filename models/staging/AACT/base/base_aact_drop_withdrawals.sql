WITH drop_withdrawals AS (
    SELECT * 
    FROM {{ source('aact', 'drop_withdrawals')  }}
),

renamed_casted AS (
    SELECT
    id::INT                                                                     AS id_drop_withdrawals
    , UPPER(REGEXP_REPLACE(TRIM(nct_id), '[^A-Za-z0-9]', ''))::VARCHAR(256)     AS nct_id

    , (CASE 
        WHEN result_group_id IS NULL THEN MD5('no_result_group' )
        ELSE MD5(result_group_id)
    END)::VARCHAR(256)                                                          AS result_group_id
    , result_group_id::INT                                                      AS result_group_name

    , (CASE 
        WHEN ctgov_group_code IS NULL THEN MD5('no_ctgov_group_code' )
        ELSE MD5(ctgov_group_code)
    END)::VARCHAR(256)                                                          AS ctgov_group_code_id
    , ctgov_group_code::VARCHAR(256)                                            AS ctgov_group_code

    , (CASE 
        WHEN period IS NULL THEN MD5('no_period' )
        ELSE MD5(period)
    END)::VARCHAR(256)                                                          AS period_id
    , period::VARCHAR(256)                                                      AS period

    , (CASE 
        WHEN reason IS NULL THEN MD5('no_reason' )
        ELSE MD5(reason)
    END)::VARCHAR(256)                                                          AS reason_id
    , reason::VARCHAR(256)                                                      AS reason

    , count::INT                                                                AS count

    FROM drop_withdrawals
)

SELECT * 
FROM renamed_casted


