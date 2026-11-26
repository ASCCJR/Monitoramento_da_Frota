select
    evaluation_id,
    freeze_id,
    model_version,
    cast(prediction_timestamp as date) as prediction_date,
    tipo_equipamento,
    count(*) as observation_count,
    sum(cast(y_episode as int)) as positive_observation_count,
    sum(cast(is_emitted_alert as int)) as emitted_alert_count,
    sum(cast(is_true_alert as int)) as true_alert_count,
    sum(cast(is_emitted_alert as int)) - sum(cast(is_true_alert as int)) as false_alert_count,
    count(distinct case when y_episode = 1 then next_episode_id end) as targetable_episode_count,
    count(distinct matched_episode_id) as matched_episode_count,
    avg(risk_score) as average_risk_score,
    max(risk_score) as maximum_risk_score
from {{ source('v2_gold_ml_outputs', 'fct_holdout_evaluation_scores') }}
group by all
