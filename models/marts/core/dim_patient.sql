-- {{ config(materialized='table') }}

select
    patient_id,
    gender, 
    gender_id,
    primary_site,
    primary_site_id,
    vital_status,
    vital_status_id,
    cause_of_death,
    cause_of_death_id,
    age_recode,
    year_of_diagnosis,
    survival_months,
    md5(lower(trim(primary_site))) as disease_id,
    ingestion_date
from {{ ref('stg_seer_patient') }}
