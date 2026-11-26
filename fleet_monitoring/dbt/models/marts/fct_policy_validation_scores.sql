select
    freeze_id,
    model_version,
    scoring_row_id,
    cast(prediction_timestamp as timestamp_ntz) as prediction_timestamp,
    tag,
    tipo_equipamento,
    cast(y_episode as tinyint) as y_episode,
    next_episode_id,
    cast(next_episode_start as timestamp_ntz) as next_episode_start,
    cast(lead_time_minutes as double) as lead_time_minutes,
    cast(risk_score as double) as risk_score,
    cast(rank_at_cutoff as int) as rank_at_cutoff,
    cast(is_policy_candidate as boolean) as is_policy_candidate,
    cast(is_emitted_alert as boolean) as is_emitted_alert,
    matched_episode_id,
    cast(is_true_alert as boolean) as is_true_alert
from {{ source('v2_ml_outputs', 'policy_validation_scores') }}
