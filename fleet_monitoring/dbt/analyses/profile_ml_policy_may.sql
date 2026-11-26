select
    freeze_id,
    min(prediction_timestamp) as first_prediction_timestamp,
    max(prediction_timestamp) as last_prediction_timestamp,
    count(*) as row_count,
    count(distinct tag) as tag_count,
    sum(y_episode) as positive_row_count,
    count(distinct case when y_episode = 1 then next_episode_id end) as targetable_episode_count,
    sum(cast(is_emitted_alert as bigint)) as emitted_alert_count,
    sum(cast(is_true_alert as bigint)) as true_alert_count,
    count(distinct matched_episode_id) as matched_episode_count,
    min(risk_score) as min_risk_score,
    max(risk_score) as max_risk_score
from {{ source('v2_ml_outputs', 'policy_validation_scores') }}
group by freeze_id
