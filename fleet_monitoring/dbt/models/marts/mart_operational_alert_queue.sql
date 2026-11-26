select
    operational_score_id as alert_id,
    scoring_run_id,
    freeze_id,
    model_version,
    prediction_timestamp,
    prediction_date,
    prediction_hour,
    tag,
    tipo_equipamento,
    risk_score,
    rank_at_cutoff,
    policy_threshold,
    policy_max_alerts_per_cutoff,
    policy_cooldown_hours,
    'SIMULATED_NOT_ACTIONABLE' as queue_status,
    scored_at_utc
from {{ ref('fct_operational_scores') }}
where is_emitted_alert
