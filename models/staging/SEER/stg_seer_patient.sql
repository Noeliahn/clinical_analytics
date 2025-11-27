{{ config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    on_schema_change = 'append_new_columns'
) }}

WITH patient AS (
    SELECT * 
    FROM {{ ref('base_seer_patient') }}
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

    {% if is_incremental() %}
        -- Solo nuevos registros basados en ingestion_date
        WHERE ingestion_date >
          (SELECT COALESCE(MAX(ingestion_date), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT *
FROM renamed_casted

{#
-- Duplicate check query (commented out)
/*
SELECT 
    patient_id,
    year_of_diagnosis,
    cause_of_death,
    primary_site,
    vital_status,
    survival_months,
    COUNT(*) AS cnt
FROM base_seer_patient
GROUP BY patient_id,
    year_of_diagnosis,
    cause_of_death,
    primary_site,
    vital_status,
    survival_months
HAVING COUNT(*) > 1
ORDER BY cnt DESC
*/
#}