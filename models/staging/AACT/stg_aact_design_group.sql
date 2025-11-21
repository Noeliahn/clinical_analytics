WITH stage_design_groups AS (
    SELECT * 
    FROM {{ ref('base_aact_design_group')  }}
),

renamed_casted AS (
    SELECT
    id_design_groups
    , nct_id
    , group_type_id
    , title
    , description
    
    FROM stage_design_groups
)

SELECT * 
FROM renamed_casted
