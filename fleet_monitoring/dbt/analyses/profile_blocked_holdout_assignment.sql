select
    split_id,
    split_role,
    count(*) as row_count,
    min(prediction_timestamp) as first_prediction_timestamp,
    max(prediction_timestamp) as last_prediction_timestamp,
    min(is_released) as any_released
from {{ ref('fct_training_assignments') }}
where is_holdout
group by split_id, split_role
