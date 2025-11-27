WITH src_seer AS (
    SELECT * 
    FROM {{ source('seer', 'seer') }}
),

renamed_casted AS (
    SELECT
        sex::VARCHAR(256)                       AS gender
        , MD5(sex)::VARCHAR(256)                  AS gender_id
        , year_of_diagnosis::VARCHAR(256)         AS year_of_diagnosis
        , site_recode::VARCHAR(256)               AS primary_site
        , MD5(site_recode)::VARCHAR(256)          AS primary_site_id
        , cause_of_death::VARCHAR(256)            AS cause_of_death
        , MD5(cause_of_death)::VARCHAR(256)       AS cause_of_death_id
        , TRY_TO_NUMBER(survival_months)::INT     AS survival_months
        , vital_status::VARCHAR(256)              AS vital_status
        , MD5(vital_status)::VARCHAR(256)         AS vital_status_id
        , TRY_TO_NUMBER(patient_id)::INT          AS patient_id
        , age_recode::VARCHAR(256)                AS age_recode
        , CURRENT_TIMESTAMP()                     AS ingestion_date
    FROM src_seer
),


deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id 
            ORDER BY ingestion_date DESC, survival_months DESC
        ) as rn
    FROM renamed_casted
)

SELECT * 
FROM deduped
WHERE rn = 1 