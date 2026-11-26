select
    min(dataset_start_time) as dataset_start_time,
    max(dataset_end_time) as dataset_end_time,
    min(prediction_timestamp) as first_prediction_timestamp,
    max(prediction_timestamp) as last_prediction_timestamp,
    count(distinct cutoff_id) as cutoff_count,
    count(distinct tag) as equipment_count,
    count(*) as grid_row_count,
    count_if(is_scoring_eligible) as scoring_eligible_row_count,
    count_if(is_training_eligible) as training_eligible_row_count,
    count_if(tag_is_cold_start) as cold_start_row_count,
    count_if(active_event_count = 0) as inactive_row_count,
    count_if(washout_occurrence_count > 0) as washout_row_count,
    count_if(has_lookback_unavailability) as lookback_unavailable_row_count,
    count_if(has_followup_unavailability) as followup_unavailable_row_count,
    count_if(not has_complete_global_lookback) as incomplete_lookback_row_count,
    count_if(not has_complete_global_followup) as incomplete_followup_row_count
from {{ ref('int_scoring_grade') }}
