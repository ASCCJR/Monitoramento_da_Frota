select *
from {{ ref('int_telemetria_features_hora_tag') }}
where
    event_count <= 0
    or critical_event_count < 0
    or noncritical_event_count < 0
    or informational_event_count < 0
    or other_event_count < 0
    or dontgo_occurrence_count < 0
    or cma_event_count < 0
    or cma_very_high_event_count < 0
    or cma_high_event_count < 0
    or critical_event_count
        + noncritical_event_count
        + informational_event_count
        + other_event_count <> event_count
    or dontgo_occurrence_count > event_count
    or cma_event_count > event_count
    or cma_very_high_event_count > cma_event_count
    or cma_high_event_count > cma_event_count
    or max_feature_event_time >= timestampadd(HOUR, 1, event_hour)
    or max_feature_availability_time < max_feature_event_time
