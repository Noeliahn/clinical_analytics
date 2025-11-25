WITH patient AS (
    SELECT * 
    FROM {{ ref('base_seer_patient')  }}
),

renamed_casted AS (
    SELECT
         gender
        , gender_id
        , year_of_diagnosis
        , primary_site
        , primary_site_id
        , cause_of_death
        , cause_of_death_id
        , survival_months
        , vital_status
        , vital_status_id
        , patient_id
        , age_recode
        , ingestion_date
    FROM patient
)

SELECT * 
FROM renamed_casted
