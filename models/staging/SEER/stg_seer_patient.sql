{{
    config(
        materialized = 'incremental',
        unique_key = 'patient_id',
        on_schema_change = 'append_new_columns'
    )
}}

-- 🔥 AHORA base_seer_patient ya viene DEDUPLICADO
SELECT
    gender,
    gender_id,
    year_of_diagnosis,
    primary_site,
    primary_site_id,
    cause_of_death,
    cause_of_death_id,
    survival_months,
    vital_status,
    vital_status_id,
    patient_id,
    age_recode,
    ingestion_date
FROM {{ ref('base_seer_patient') }}
{% if is_incremental() %}
    WHERE ingestion_date > 
      (SELECT COALESCE(MAX(ingestion_date), '1900-01-01') FROM {{ this }})
{% endif %}