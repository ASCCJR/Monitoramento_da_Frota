select *
from {{ ref('fct_features_scoring') }}
where
    max_feature_event_time_24h >= prediction_timestamp
    or max_feature_availability_time_24h > prediction_timestamp
    or hours_since_last_episode_start < 0
    or episode_history_exposure_days < 0
