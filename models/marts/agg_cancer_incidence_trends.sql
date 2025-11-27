-- models/gold/agg_cancer_incidence_trends.sql
{{
    config(
        materialized = 'table',
        tags = ['gold', 'aggregation', 'trends']
    )
}}

SELECT
    year_of_diagnosis,
    primary_site as cancer_type,
    gender,
    age_recode,
    
    -- Métricas agregadas
    COUNT(*) as patient_count,
    AVG(survival_months) as avg_survival_months,
    COUNT(CASE WHEN vital_status = 'Dead' THEN 1 END) as deceased_count,
    COUNT(CASE WHEN vital_status = 'Alive' THEN 1 END) as alive_count,
    
    -- Tasas calculadas
    ROUND(deceased_count * 100.0 / COUNT(*), 2) as mortality_rate_percent,
    ROUND(AVG(survival_months) / 12, 1) as avg_survival_years

FROM {{ ref('dim_patient') }}
WHERE year_of_diagnosis IS NOT NULL
GROUP BY year_of_diagnosis, primary_site, gender, age_recode
ORDER BY year_of_diagnosis DESC, patient_count DESC