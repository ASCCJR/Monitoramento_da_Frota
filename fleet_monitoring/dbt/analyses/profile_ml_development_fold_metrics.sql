select
    freeze_id,
    model_name,
    split_id,
    max(case when metric_name = 'row_count' then metric_value end) as row_count,
    max(case when metric_name = 'positive_count' then metric_value end) as positive_count,
    max(case when metric_name = 'prevalence' then metric_value end) as prevalence,
    max(case when metric_name = 'average_precision' then metric_value end) as average_precision,
    max(case when metric_name = 'roc_auc' then metric_value end) as roc_auc,
    max(case when metric_name = 'brier' then metric_value end) as brier
from {{ source('v2_ml_outputs', 'development_fold_metrics') }}
where split_id <> 'macro'
group by freeze_id, model_name, split_id
order by model_name, split_id
