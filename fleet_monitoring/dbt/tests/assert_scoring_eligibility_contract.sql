select *
from {{ ref('int_scoring_grade') }}
where
    (is_scoring_eligible and active_event_count <= 0)
    or (is_scoring_eligible and washout_occurrence_count <> 0)
    or (is_scoring_eligible and has_lookback_unavailability)
    or (is_scoring_eligible and not has_complete_global_lookback)
    or (is_training_eligible and not is_scoring_eligible)
    or (is_training_eligible and has_followup_unavailability)
    or (is_training_eligible and not has_complete_global_followup)
