-- models/gold/fct_patient_survival_analysis.sql
--fct_patient_survival_analysis
{{
    config(
        materialized = 'table',
        tags = ['gold', 'survival_analysis']
    )
}}

SELECT
    -- Claves
    dp.patient_id,
    dd.disease_id,
    
    -- Dimensiones de análisis
    dp.gender,
    dp.age_recode,
    dp.year_of_diagnosis,
    dd.disease_name_normalized as cancer_type,
    
    -- Métricas de supervivencia
    dp.survival_months,
    dp.vital_status,
    
    -- Categorías para análisis
    CASE 
        WHEN dp.survival_months < 12 THEN '<1 año'
        WHEN dp.survival_months BETWEEN 12 AND 60 THEN '1-5 años'
        ELSE '5+ años'
    END as survival_category,
    
    -- Flags para análisis
    CASE WHEN dp.vital_status = 'Dead' THEN 1 ELSE 0 END as is_deceased,
    CASE WHEN dp.survival_months > 60 THEN 1 ELSE 0 END as survived_5_years,
    
    -- Metadata
    dp.ingestion_date

FROM {{ ref('dim_patient') }} dp
LEFT JOIN {{ ref('dim_disease') }} dd ON dp.disease_id = dd.disease_id
WHERE dp.survival_months IS NOT NULL