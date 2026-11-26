with summary as (
    select
        freeze_id,
        count(*) as row_count,
        count(distinct feature_name) as feature_count,
        count(distinct importance_rank) as rank_count,
        sum(importance_normalized) as normalized_sum,
        min(importance_normalized) as minimum_importance
    from {{ ref('dim_model_feature_importance') }}
    group by freeze_id
)

select *
from summary
where row_count <> 46
    or feature_count <> 46
    or rank_count <> 46
    or abs(normalized_sum - 1.0) > 1e-12
    or minimum_importance < 0
