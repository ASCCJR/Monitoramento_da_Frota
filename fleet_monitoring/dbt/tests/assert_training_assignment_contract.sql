select *
from {{ ref('fct_training_assignments') }}
where
    prediction_timestamp < period_start
    or prediction_timestamp >= period_end_exclusive
    or label_window_end > period_end_exclusive
    or (is_holdout and is_released)
    or (not is_holdout and not is_released)
    or (split_role = 'holdout' and split_id <> 'holdout_june')
    or (split_role <> 'holdout' and prediction_timestamp >= timestamp_ntz'2025-06-01 00:00:00')
