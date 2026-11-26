with scores as (
    select *
    from {{ source('v2_gold_ml_outputs', 'fct_holdout_evaluation_scores') }}
),

score_metrics as (
    select
        evaluation_id,
        count(*) as row_count,
        sum(y_episode) as positive_count,
        sum(cast(is_emitted_alert as bigint)) as alert_count,
        sum(cast(is_true_alert as bigint)) as true_alert_count,
        count(distinct case when y_episode = 1 then next_episode_id end) as targetable_episode_count,
        count(distinct matched_episode_id) as matched_episode_count
    from scores
    group by evaluation_id
),

metrics as (
    select
        evaluation_id,
        max(case when scope = 'observation' and segment_type = 'overall' and metric_name = 'row_count' then metric_value end) as row_count,
        max(case when scope = 'observation' and segment_type = 'overall' and metric_name = 'positive_count' then metric_value end) as positive_count,
        max(case when scope = 'operational' and segment_type = 'overall' and metric_name = 'alert_count' then metric_value end) as alert_count,
        max(case when scope = 'operational' and segment_type = 'overall' and metric_name = 'true_alert_count' then metric_value end) as true_alert_count,
        max(case when scope = 'operational' and segment_type = 'overall' and metric_name = 'targetable_episode_count' then metric_value end) as targetable_episode_count,
        max(case when scope = 'operational' and segment_type = 'overall' and metric_name = 'matched_episode_count' then metric_value end) as matched_episode_count
    from {{ source('v2_ml_outputs', 'holdout_evaluation_metrics') }}
    group by evaluation_id
)

select score_metrics.evaluation_id
from score_metrics
left join metrics using (evaluation_id)
where
    metrics.evaluation_id is null
    or score_metrics.row_count <> metrics.row_count
    or score_metrics.positive_count <> metrics.positive_count
    or score_metrics.alert_count <> metrics.alert_count
    or score_metrics.true_alert_count <> metrics.true_alert_count
    or score_metrics.targetable_episode_count <> metrics.targetable_episode_count
    or score_metrics.matched_episode_count <> metrics.matched_episode_count
