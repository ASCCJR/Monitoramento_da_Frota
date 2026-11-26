select
    freeze_id,
    model_name,
    max(case when metric_name = 'average_precision' then metric_value end) as macro_average_precision,
    max(case when metric_name = 'roc_auc' then metric_value end) as macro_roc_auc,
    max(case when metric_name = 'brier' then metric_value end) as macro_brier
from {{ source('v2_ml_outputs', 'development_fold_metrics') }}
where split_id = 'macro'
group by freeze_id, model_name
order by macro_average_precision desc
