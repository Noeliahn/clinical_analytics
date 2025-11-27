{% snapshot patient_snp %}

{{
    config(
        target_schema='snapshots',
        unique_key='patient_id',         
        strategy='check', 
        check_cols=['vital_status', 'cause_of_death', 'survival_months', 'year_of_diagnosis'],
        invalidate_hard_deletes=True

    )
}}

SELECT
     gender,
     year_of_diagnosis,
     primary_site,
     cause_of_death,
     survival_months,
     vital_status,
     patient_id,
     age_recode,
     ingestion_date
FROM {{ ref('stg_seer_patient') }}

{% endsnapshot %}