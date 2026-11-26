select
    (select max(typeof(frozen_at_utc))
     from {{ source('v2_ml_outputs', 'model_freeze_manifest') }}) as freeze_timestamp_type,
    (select max(typeof(prediction_timestamp))
     from {{ source('v2_ml_outputs', 'policy_validation_scores') }}) as policy_timestamp_type,
    (select max(typeof(next_episode_start))
     from {{ source('v2_ml_outputs', 'policy_validation_scores') }}) as episode_timestamp_type
