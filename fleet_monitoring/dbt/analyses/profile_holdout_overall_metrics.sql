select
    evaluation_id,
    scope,
    max(case when metric_name = 'row_count' then metric_value end) as row_count,
    max(case when metric_name = 'positive_count' then metric_value end) as positive_count,
    max(case when metric_name = 'prevalence' then metric_value end) as prevalence,
    max(case when metric_name = 'average_precision' then metric_value end) as average_precision,
    max(case when metric_name = 'roc_auc' then metric_value end) as roc_auc,
    max(case when metric_name = 'brier' then metric_value end) as brier,
    max(case when metric_name = 'precision' then metric_value end) as observation_precision,
    max(case when metric_name = 'recall' then metric_value end) as observation_recall,
    max(case when metric_name = 'f1' then metric_value end) as observation_f1,
    max(case when metric_name = 'lift_over_prevalence' then metric_value end) as lift_over_prevalence,
    max(case when metric_name = 'alert_count' then metric_value end) as alert_count,
    max(case when metric_name = 'true_alert_count' then metric_value end) as true_alert_count,
    max(case when metric_name = 'episode_precision' then metric_value end) as episode_precision,
    max(case when metric_name = 'targetable_episode_recall' then metric_value end) as targetable_episode_recall,
    max(case when metric_name = 'total_observed_episode_recall' then metric_value end) as total_observed_episode_recall,
    max(case when metric_name = 'episode_f1' then metric_value end) as episode_f1,
    max(case when metric_name = 'median_lead_time_minutes' then metric_value end) as median_lead_time_minutes,
    max(case when metric_name = 'alerts_per_active_cutoff' then metric_value end) as alerts_per_active_cutoff,
    max(case when metric_name = 'alerts_per_active_day' then metric_value end) as alerts_per_active_day,
    max(case when metric_name = 'targetable_episode_count' then metric_value end) as targetable_episode_count,
    max(case when metric_name = 'matched_episode_count' then metric_value end) as matched_episode_count,
    max(case when metric_name = 'observed_episode_count' then metric_value end) as observed_episode_count
from {{ source('v2_ml_outputs', 'holdout_evaluation_metrics') }}
where segment_type = 'overall'
group by evaluation_id, scope
order by scope
