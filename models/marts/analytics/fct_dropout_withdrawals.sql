-- {{ config(materialized='incremental', unique_key='id_drop_withdrawals') }}

select
    id_drop_withdrawals,
    nct_id,
    result_group_id,
    ctgov_group_code_id,
    period_id,
    reason_id,
    count as withdrawal_count
from {{ ref('stg_aact_drop_withdrawals') }}

-- {% if is_incremental() %}
-- where updated_at > (select max(updated_at) from {{ this }})
-- {% endif %};
