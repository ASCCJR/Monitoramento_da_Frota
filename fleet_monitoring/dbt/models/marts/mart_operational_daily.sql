select
    scoring_run_id,
    freeze_id,
    model_version,
    prediction_date,
    tipo_equipamento,
    count(*) as scored_equipment_count,
    count(distinct tag) as scored_tag_count,
    sum(cast(is_policy_candidate as int)) as policy_candidate_count,
    sum(cast(is_emitted_alert as int)) as emitted_alert_count,
    avg(risk_score) as average_risk_score,
    max(risk_score) as maximum_risk_score,
    percentile_approx(risk_score, 0.95, 10000) as p95_risk_score
from {{ ref('fct_operational_scores') }}
group by all
