-- QUERY CORREGIDA PARA LANDSCAPE ONCOLÓGICO
WITH survival_data AS (
  SELECT 
    dp.primary_site as cancer_type,
    dp.year_of_diagnosis,
    dp.gender,
    dp.age_recode,
    COUNT(*) as total_pacientes,
    AVG(dp.survival_months) as supervivencia_promedio_meses,
    
    -- Cálculo CORRECTO de supervivientes a 5 años
    COUNT(CASE WHEN dp.survival_months >= 60 THEN 1 END) as supervivientes_5_anos,
    
    -- Categoría de edad limpia
    CASE 
      WHEN dp.age_recode LIKE '%01-04%' THEN '0-4'
      WHEN dp.age_recode LIKE '%05-09%' THEN '5-9'
      WHEN dp.age_recode LIKE '%10-14%' THEN '10-14'
      WHEN dp.age_recode LIKE '%15-19%' THEN '15-19'
      WHEN dp.age_recode LIKE '%20-24%' THEN '20-24'
      WHEN dp.age_recode LIKE '%25-29%' THEN '25-29'
      WHEN dp.age_recode LIKE '%30-34%' THEN '30-34'
      WHEN dp.age_recode LIKE '%35-39%' THEN '35-39'
      WHEN dp.age_recode LIKE '%40-44%' THEN '40-44'
      WHEN dp.age_recode LIKE '%45-49%' THEN '45-49'
      WHEN dp.age_recode LIKE '%50-54%' THEN '50-54'
      WHEN dp.age_recode LIKE '%55-59%' THEN '55-59'
      WHEN dp.age_recode LIKE '%60-64%' THEN '60-64'
      WHEN dp.age_recode LIKE '%65-69%' THEN '65-69'
      WHEN dp.age_recode LIKE '%70-74%' THEN '70-74'
      WHEN dp.age_recode LIKE '%75-79%' THEN '75-79'
      WHEN dp.age_recode LIKE '%80-84%' THEN '80-84'
      WHEN dp.age_recode LIKE '%85-89%' THEN '85-89'
      WHEN dp.age_recode LIKE '%90+%' THEN '90+'
      ELSE dp.age_recode
    END as grupo_edad_limpio
    
  FROM {{ ref('dim_patient') }} dp
  WHERE dp.year_of_diagnosis BETWEEN 1980 AND 2023
    AND dp.survival_months IS NOT NULL
    AND dp.primary_site IS NOT NULL
  GROUP BY dp.primary_site, dp.year_of_diagnosis, dp.gender, dp.age_recode, grupo_edad_limpio
)

SELECT 
  cancer_type,
  year_of_diagnosis,
  gender,
  age_recode,
  grupo_edad_limpio,
  total_pacientes,
  supervivencia_promedio_meses,
  supervivientes_5_anos,
  
  -- Tasa de supervivencia a 5 años (evitar división por 0)
  CASE 
    WHEN total_pacientes > 0 THEN 
      ROUND((supervivientes_5_anos * 100.0 / total_pacientes), 1)
    ELSE 0 
  END as tasa_supervivencia_5_anos,
  
  -- Años desde diagnóstico (para tendencias)
  year_of_diagnosis - 1980 as anos_desde_inicio_estudio

FROM survival_data
WHERE total_pacientes >= 1  -- Evitar grupos muy pequeños
ORDER BY year_of_diagnosis, total_pacientes DESC