with comparison as (
    select
        operational.scoring_row_id,
        abs(operational.risk_score - holdout.risk_score) as score_difference,
        operational.rank_at_cutoff <> holdout.rank_at_cutoff as rank_differs,
        operational.is_policy_candidate <> holdout.is_policy_candidate as candidate_differs,
        operational.is_emitted_alert <> holdout.is_emitted_alert as alert_differs
    from {{ ref('fct_operational_scores') }} as operational
    inner join {{ source('v2_gold_ml_outputs', 'fct_holdout_evaluation_scores') }} as holdout
        on operational.freeze_id = holdout.freeze_id
        and operational.scoring_row_id = holdout.scoring_row_id
    where operational.score_start = timestamp_ntz'2025-06-01 00:00:00'
        and operational.score_end = timestamp_ntz'2025-07-01 00:00:00'
)

select *
from comparison
where score_difference > 1e-15 or rank_differs or candidate_differs or alert_differs
