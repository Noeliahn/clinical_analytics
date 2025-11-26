{{ config(severity = 'warn', tags = ['data_integrity']) }}

-- Test de Integridad entre SEER y AACT
-- Valida que las enfermedades en AACT existan en el catálogo maestro
WITH aact_diseases_not_in_master AS (
    SELECT DISTINCT
        dc.condition_name as aact_disease,
        'AACT' as source
    FROM {{ ref('dim_condition') }} dc
    LEFT JOIN {{ ref('dim_disease') }} dd 
        ON LOWER(dc.condition_name) = LOWER(dd.disease_name_normalized)
    WHERE dd.disease_id IS NULL
),

seer_diseases_not_in_master AS (
    SELECT DISTINCT
        dp.primary_site as seer_disease,
        'SEER' as source
    FROM {{ ref('dim_patient') }} dp
    LEFT JOIN {{ ref('dim_disease') }} dd 
        ON LOWER(dp.primary_site) = LOWER(dd.disease_name_normalized)
    WHERE dd.disease_id IS NULL
)

SELECT * FROM aact_diseases_not_in_master
UNION ALL
SELECT * FROM seer_diseases_not_in_master