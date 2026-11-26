select
    evaluation_id,
    segment_value as k,
    max(case when metric_name = 'selected_row_count' then metric_value end) as selected_row_count,
    max(case when metric_name = 'positive_row_count' then metric_value end) as positive_row_count,
    max(case when metric_name = 'precision_at_k' then metric_value end) as precision_at_k,
    max(case when metric_name = 'recall_at_k' then metric_value end) as recall_at_k,
    max(case when metric_name = 'lift_at_k' then metric_value end) as lift_at_k
from {{ source('v2_ml_outputs', 'holdout_evaluation_metrics') }}
where scope = 'ranking' and segment_type = 'top_k'
group by evaluation_id, segment_value
order by cast(segment_value as int)
