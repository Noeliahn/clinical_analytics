-- models/gold/agg_landscape_oncologico.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dashboard', 'landscape']
    )
}}

WITH base_data AS (
  SELECT 
    -- Dimensiones principales
    cancer_type,
    year_of_diagnosis,
    gender,
    age_recode,
    survival_months,
    survived_5_years,
    is_deceased,
    survival_category,
    
    -- Grupo de edad limpio para visualizaciones
    CASE 
      WHEN age_recode LIKE '%01-04%' THEN '0-4'
      WHEN age_recode LIKE '%05-09%' THEN '5-9'
      WHEN age_recode LIKE '%10-14%' THEN '10-14'
      WHEN age_recode LIKE '%15-19%' THEN '15-19'
      WHEN age_recode LIKE '%20-24%' THEN '20-24'
      WHEN age_recode LIKE '%25-29%' THEN '25-29'
      WHEN age_recode LIKE '%30-34%' THEN '30-34'
      WHEN age_recode LIKE '%35-39%' THEN '35-39'
      WHEN age_recode LIKE '%40-44%' THEN '40-44'
      WHEN age_recode LIKE '%45-49%' THEN '45-49'
      WHEN age_recode LIKE '%50-54%' THEN '50-54'
      WHEN age_recode LIKE '%55-59%' THEN '55-59'
      WHEN age_recode LIKE '%60-64%' THEN '60-64'
      WHEN age_recode LIKE '%65-69%' THEN '65-69'
      WHEN age_recode LIKE '%70-74%' THEN '70-74'
      WHEN age_recode LIKE '%75-79%' THEN '75-79'
      WHEN age_recode LIKE '%80-84%' THEN '80-84'
      WHEN age_recode LIKE '%85-89%' THEN '85-89'
      WHEN age_recode LIKE '%90+%' THEN '90+'
      ELSE 'No especificado'
    END as grupo_edad_limpio,
    
    -- Década para análisis temporal
    CASE 
      WHEN year_of_diagnosis < 1990 THEN '1980-1989'
      WHEN year_of_diagnosis BETWEEN 1990 AND 1999 THEN '1990-1999'
      WHEN year_of_diagnosis BETWEEN 2000 AND 2009 THEN '2000-2009'
      WHEN year_of_diagnosis BETWEEN 2010 AND 2019 THEN '2010-2019'
      ELSE '2020+'
    END as decada_diagnostico

  FROM {{ ref('fct_patient_survival_analysis') }}
  WHERE cancer_type IS NOT NULL 
    AND year_of_diagnosis BETWEEN 1980 AND 2023
    AND survival_months IS NOT NULL
),

aggregated AS (
  SELECT 
    cancer_type,
    year_of_diagnosis,
    decada_diagnostico,
    gender,
    grupo_edad_limpio,
    
    -- Métricas principales
    COUNT(*) as total_pacientes,
    SUM(survived_5_years) as supervivientes_5_anos,
    AVG(survival_months) as supervivencia_promedio_meses,
    SUM(is_deceased) as fallecidos,
    
    -- Distribución categorías supervivencia
    COUNT(CASE WHEN survival_category = '<1 año' THEN 1 END) as menos_1_ano,
    COUNT(CASE WHEN survival_category = '1-5 años' THEN 1 END) as entre_1_5_anos,
    COUNT(CASE WHEN survival_category = '5+ años' THEN 1 END) as mas_5_anos

  FROM base_data
  GROUP BY cancer_type, year_of_diagnosis, decada_diagnostico, gender, grupo_edad_limpio
)

SELECT 
  *,
  -- Tasas calculadas (con protección división por cero)
  CASE 
    WHEN total_pacientes > 0 THEN 
      ROUND((supervivientes_5_anos * 100.0 / total_pacientes), 2)
    ELSE 0 
  END as tasa_supervivencia_5_anos,
  
  CASE 
    WHEN total_pacientes > 0 THEN 
      ROUND((fallecidos * 100.0 / total_pacientes), 2)
    ELSE 0 
  END as tasa_mortalidad,
  
  -- Supervivencia en años
  ROUND(supervivencia_promedio_meses / 12, 1) as supervivencia_promedio_anos,
  
  -- Flags para filtros
  CASE WHEN total_pacientes >= 100 THEN 'Alto volumen' 
       WHEN total_pacientes >= 50 THEN 'Medio volumen'
       ELSE 'Bajo volumen' 
  END as volumen_pacientes

FROM aggregated
WHERE total_pacientes >= 5  -- Filtramos grupos muy pequeños
ORDER BY year_of_diagnosis DESC, total_pacientes DESC