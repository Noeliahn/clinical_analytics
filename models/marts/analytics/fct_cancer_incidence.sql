-- Tendencias temporales de cáncer
-- fct_cancer_incidence
SELECT
    year_of_diagnosis,
    primary_site as cancer_type,
    gender,
    age_recode,
    COUNT(*) as patient_count,
    AVG(survival_months) as avg_survival_months,
    COUNT(CASE WHEN vital_status = 'Dead' THEN 1 END) as deceased_count
FROM {{ ref('dim_patient') }}
GROUP BY year_of_diagnosis, primary_site, gender, age_recode