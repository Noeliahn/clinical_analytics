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
    ingestion_date, 
    -- En tu dim_patient actual, añadir columnas calculadas:
-- models/gold/dim_patient.sql (versión mejorada)

-- Añadir al SELECT:
    -- Métricas de supervivencia
    CASE 
        WHEN survival_months >= 60 THEN 'long_term_survivor'
        WHEN survival_months BETWEEN 12 AND 60 THEN 'medium_term_survivor' 
        WHEN survival_months < 12 THEN 'short_term_survivor'
        ELSE 'unknown'
    END as survival_category,
    
    -- Edad numérica para análisis
    CASE 
        WHEN age_recode LIKE '%01-04%' THEN 2
        WHEN age_recode LIKE '%05-09%' THEN 7
        WHEN age_recode LIKE '%10-14%' THEN 12
        -- ... completar mapeo
        WHEN age_recode LIKE '%85-89%' THEN 87
        WHEN age_recode LIKE '%90+%' THEN 90
        ELSE NULL
    END as approximate_age,
    
    -- Periodo de diagnóstico
    CASE 
        WHEN year_of_diagnosis < 1990 THEN 'pre_1990'
        WHEN year_of_diagnosis BETWEEN 1990 AND 2000 THEN '1990s'
        WHEN year_of_diagnosis BETWEEN 2000 AND 2010 THEN '2000s'
        WHEN year_of_diagnosis > 2010 THEN 'post_2010'
        ELSE 'unknown'
    END as diagnosis_period
from {{ ref('stg_seer_patient') }}
