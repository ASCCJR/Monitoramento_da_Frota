select
    evaluation_id,
    release_id,
    freeze_id,
    scope,
    segment_type,
    segment_value,
    metric_name,
    cast(metric_value as double) as metric_value,
    case metric_name
        when 'average_precision' then 'AUC-PR'
        when 'roc_auc' then 'AUC-ROC'
        when 'brier' then 'Brier score'
        when 'lift_over_prevalence' then 'Lift sobre prevalencia'
        when 'episode_precision' then 'Precisao por alerta'
        when 'targetable_episode_recall' then 'Recall de episodios targetaveis'
        when 'total_observed_episode_recall' then 'Recall de episodios observados'
        when 'median_lead_time_minutes' then 'Antecedencia mediana (min)'
        when 'alert_count' then 'Alertas emitidos'
        when 'true_alert_count' then 'Alertas verdadeiros'
        when 'false_alert_count' then 'Alertas falsos'
        when 'targetable_episode_count' then 'Episodios targetaveis'
        when 'matched_episode_count' then 'Episodios associados'
        when 'prevalence' then 'Prevalencia'
        when 'row_count' then 'Observacoes'
        when 'precision_at_k' then 'Precisao no top-K'
        when 'recall_at_k' then 'Recall no top-K'
        when 'lift_at_k' then 'Lift no top-K'
        else metric_name
    end as metric_display_name,
    case
        when metric_name in (
            'average_precision', 'roc_auc', 'brier', 'episode_precision',
            'targetable_episode_recall', 'total_observed_episode_recall',
            'prevalence', 'precision_at_k', 'recall_at_k'
        ) then 'RATIO'
        when metric_name = 'median_lead_time_minutes' then 'MINUTES'
        when metric_name like '%count' or metric_name in ('row_count', 'k') then 'COUNT'
        else 'NUMBER'
    end as metric_format
from {{ source('v2_ml_outputs', 'holdout_evaluation_metrics') }}
