with violations as (
    select scoring_row_id
    from {{ ref('fct_policy_validation_scores') }}
    where
        typeof(prediction_timestamp) <> 'timestamp_ntz'
        or (
            next_episode_start is not null
            and typeof(next_episode_start) <> 'timestamp_ntz'
        )
        or risk_score < 0
        or risk_score > 1
        or (is_true_alert and not is_emitted_alert)
        or (is_emitted_alert and not is_policy_candidate)
        or (is_true_alert and matched_episode_id is null)
        or (not is_true_alert and matched_episode_id is not null)
),

duplicates as (
    select freeze_id, scoring_row_id
    from {{ ref('fct_policy_validation_scores') }}
    group by freeze_id, scoring_row_id
    having count(*) <> 1
)

select scoring_row_id
from violations

union all

select scoring_row_id
from duplicates
