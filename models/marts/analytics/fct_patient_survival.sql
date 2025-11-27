--fct_patient_survival
-- Análisis de supervivencia por tipo de cáncer
SELECT 
    dp.patient_id,
    dp.gender,
    dp.primary_site as cancer_type,
    dp.year_of_diagnosis,
    dp.vital_status,
    dp.survival_months,
    dp.age_recode,
    dd.disease_name_normalized,
    -- Métricas calculadas
    CASE 
        WHEN dp.survival_months >= 60 THEN '5+ años supervivencia'
        WHEN dp.survival_months >= 12 THEN '1-5 años supervivencia' 
        ELSE '<1 año supervivencia'
    END as survival_category
FROM {{ ref('dim_patient') }} dp
LEFT JOIN {{ ref('dim_disease') }} dd ON dp.disease_id = dd.disease_id