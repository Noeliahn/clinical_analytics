-- models/gold/agg_geographic_research.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dashboard', 'research_geography']
    )
}}

WITH country_research AS (
  SELECT 
    sf.country,
    sf.country_id,
    
    -- Métricas de investigación (usando tablas que SÍ existen)
    COUNT(DISTINCT sf.nct_id) as total_estudios_clinicos,
    COUNT(DISTINCT sf.id_facilities) as total_centros_investigacion,
    
    -- Tipos de cáncer desde dim_condition (que SÍ existe)
    COUNT(DISTINCT dc.condition_name) as tipos_cancer_estudiados,
    
    -- Intervenciones por país
    COUNT(DISTINCT di.intervention_name) as total_intervenciones
    
  FROM {{ ref('stg_aact_facilities') }} sf
  LEFT JOIN {{ ref('dim_condition') }} dc ON sf.nct_id = dc.nct_id
  LEFT JOIN {{ ref('dim_intervention') }} di ON sf.nct_id = di.nct_id
  WHERE sf.country IS NOT NULL 
    AND sf.country != 'no_country'
  GROUP BY sf.country, sf.country_id
),

cancer_focus AS (
  SELECT 
    sf.country,
    dc.condition_name as cancer_type,
    COUNT(DISTINCT sf.nct_id) as estudios_por_cancer
  FROM {{ ref('stg_aact_facilities') }} sf
  LEFT JOIN {{ ref('dim_condition') }} dc ON sf.nct_id = dc.nct_id
  WHERE dc.condition_name IS NOT NULL
  GROUP BY sf.country, dc.condition_name
),

top_cancers AS (
  SELECT 
    country,
    cancer_type,
    estudios_por_cancer,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY estudios_por_cancer DESC) as rank_cancer
  FROM cancer_focus
  WHERE estudios_por_cancer >= 1
)

SELECT 
  cr.*,
  tc.cancer_type as cancer_principal,
  tc.estudios_por_cancer as estudios_cancer_principal,
  
  -- Densidad de investigación (con protección división por cero)
  CASE 
    WHEN cr.total_centros_investigacion > 0 THEN
      ROUND(cr.total_estudios_clinicos * 1.0 / cr.total_centros_investigacion, 2)
    ELSE 0
  END as estudios_por_centro,
  
  -- Diversidad de investigación (con protección división por cero)
  CASE 
    WHEN cr.total_estudios_clinicos > 0 THEN
      ROUND(cr.tipos_cancer_estudiados * 1.0 / cr.total_estudios_clinicos, 2)
    ELSE 0
  END as diversidad_investigacion,

  -- Categorización por nivel de actividad
  CASE 
    WHEN cr.total_estudios_clinicos >= 100 THEN 'Alta actividad'
    WHEN cr.total_estudios_clinicos >= 50 THEN 'Media actividad' 
    WHEN cr.total_estudios_clinicos >= 10 THEN 'Baja actividad'
    ELSE 'Muy baja actividad'
  END as nivel_actividad

FROM country_research cr
LEFT JOIN top_cancers tc ON cr.country = tc.country AND tc.rank_cancer = 1
WHERE cr.total_estudios_clinicos >= 1
ORDER BY cr.total_estudios_clinicos DESC