with scores as (
    select *
    from {{ source('v2_gold_ml_outputs', 'fct_holdout_evaluation_scores') }}
),

violations as (
    select scoring_row_id
    from scores
    where
        typeof(prediction_timestamp) <> 'timestamp_ntz'
        or (
            next_episode_start is not null
            and typeof(next_episode_start) <> 'timestamp_ntz'
        )
        or prediction_timestamp < timestamp_ntz'2025-06-01 00:00:00'
        or prediction_timestamp >= timestamp_ntz'2025-07-01 00:00:00'
        or risk_score < 0
        or risk_score > 1
        or model_version <> '1'
        or policy_threshold <> 0.2698849635742829
        or policy_max_alerts_per_cutoff <> 5
        or policy_cooldown_hours <> 4
        or (is_true_alert and not is_emitted_alert)
        or (is_emitted_alert and not is_policy_candidate)
        or (is_true_alert and matched_episode_id is null)
        or (not is_true_alert and matched_episode_id is not null)
),

duplicates as (
    select evaluation_id, scoring_row_id
    from scores
    group by evaluation_id, scoring_row_id
    having count(*) <> 1
),

assignment_reconciliation as (
    select
        count(*) as assignment_count,
        count(scores.scoring_row_id) as scored_count
    from {{ ref('fct_training_assignments') }} as assignments
    left join scores using (scoring_row_id)
    where
        assignments.split_id = 'holdout_june'
        and assignments.split_role = 'holdout'
)

select scoring_row_id
from violations

union all

select scoring_row_id
from duplicates

union all

select 'assignment_cardinality_mismatch'
from assignment_reconciliation
where assignment_count <> 17203 or scored_count <> assignment_count
