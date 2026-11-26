select
    release_id,
    freeze_id,
    released_at_utc,
    released_by,
    release_scope,
    model_name,
    model_version,
    policy_threshold,
    policy_max_alerts_per_cutoff,
    policy_cooldown_hours,
    job_run_id,
    task_run_id,
    evaluation_run_id,
    aggregate_labels_previously_exposed,
    predictions_viewed_before_release
from {{ source('v2_ml_outputs', 'holdout_release_manifest') }}
order by released_at_utc
