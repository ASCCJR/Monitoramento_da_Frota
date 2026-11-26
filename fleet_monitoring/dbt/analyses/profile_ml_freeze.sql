select
    freeze_id,
    frozen_at_utc,
    holdout_released,
    model_name,
    model_version,
    model_alias,
    selection_run_id,
    final_run_id,
    selected_algorithm,
    feature_set_version,
    feature_registry_sha256,
    modeling_code_sha256,
    training_notebook_sha256,
    model_artifact_sha256,
    policy_threshold,
    policy_max_alerts_per_cutoff,
    policy_cooldown_hours
from {{ source('v2_ml_outputs', 'model_freeze_manifest') }}
order by frozen_at_utc desc
