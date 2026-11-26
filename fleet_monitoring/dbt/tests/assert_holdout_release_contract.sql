with release as (
    select *
    from {{ source('v2_ml_outputs', 'holdout_release_manifest') }}
),

freeze as (
    select *
    from {{ source('v2_ml_outputs', 'model_freeze_manifest') }}
)

select release.release_id
from release
left join freeze using (freeze_id)
where
    freeze.freeze_id is null
    or release.release_scope <> 'holdout_june_single_evaluation'
    or release.model_name <> freeze.model_name
    or release.model_version <> freeze.model_version
    or release.policy_threshold <> freeze.policy_threshold
    or release.policy_max_alerts_per_cutoff <> freeze.policy_max_alerts_per_cutoff
    or release.policy_cooldown_hours <> freeze.policy_cooldown_hours
    or release.predictions_viewed_before_release
    or not release.aggregate_labels_previously_exposed
    or release.job_run_id rlike '\\{\\{'
    or release.task_run_id rlike '\\{\\{'
