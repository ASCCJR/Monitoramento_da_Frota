select
    evaluation_id,
    release_id,
    freeze_id,
    min(prediction_timestamp) as first_prediction_timestamp,
    max(prediction_timestamp) as last_prediction_timestamp,
    max(typeof(prediction_timestamp)) as prediction_timestamp_type,
    max(typeof(next_episode_start)) as next_episode_start_type,
    count(*) as row_count,
    count(distinct scoring_row_id) as scoring_row_count,
    count(distinct tag) as tag_count,
    sum(y_episode) as positive_row_count,
    count(distinct case when y_episode = 1 then next_episode_id end) as targetable_episode_count,
    sum(cast(is_emitted_alert as bigint)) as emitted_alert_count,
    sum(cast(is_true_alert as bigint)) as true_alert_count,
    count(distinct matched_episode_id) as matched_episode_count,
    min(risk_score) as min_risk_score,
    max(risk_score) as max_risk_score,
    min(model_version) as model_version,
    min(policy_threshold) as policy_threshold,
    min(policy_max_alerts_per_cutoff) as policy_max_alerts_per_cutoff,
    min(policy_cooldown_hours) as policy_cooldown_hours
from {{ source('v2_gold_ml_outputs', 'fct_holdout_evaluation_scores') }}
group by evaluation_id, release_id, freeze_id
