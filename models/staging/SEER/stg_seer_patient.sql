WITH patient AS (
    SELECT * 
    FROM {{ ref('base_seer_patient')  }}
),

renamed_casted AS (
    SELECT
         gender
        , year_of_diagnosis
        , primary_site
        , cause_of_death
        , survival_months
        , vital_status
        , patient_id
        , age_recode
        , ingestion_date
    FROM patient
)

SELECT * 
FROM renamed_casted
