import hashlib
import json

import pytest

from fleet_monitoring.src.model_freeze import (
    parse_training_freeze,
    training_freeze_id,
)


@pytest.fixture
def freeze():
    manifest = {
        "manifest_version": "1.0.0",
        "model_name": "reproduction.fleet_ml.dont_go_episode_risk",
        "model_version": "1",
        "final_run_id": "train-run",
        "feature_set_version": "1.0.0",
        "feature_names": ["first_feature", "second_feature"],
        "feature_count": 2,
        "holdout_released": False,
        "holdout": {"expected_row_count": 17203, "split_id": "holdout_june"},
        "policy": {"threshold": 0.27, "max_alerts_per_cutoff": 5, "cooldown_hours": 4},
        "sha256": {
            "feature_registry": "a" * 64,
            "modeling_code": "b" * 64,
            "training_notebook": "c" * 64,
            "model_artifact_tree": "d" * 64,
        },
    }
    manifest["freeze_id"] = training_freeze_id(manifest)
    row = {
        "freeze_id": manifest["freeze_id"],
        "model_name": manifest["model_name"],
        "model_version": manifest["model_version"],
        "final_run_id": manifest["final_run_id"],
        "feature_set_version": manifest["feature_set_version"],
        "feature_registry_sha256": manifest["sha256"]["feature_registry"],
        "modeling_code_sha256": manifest["sha256"]["modeling_code"],
        "training_notebook_sha256": manifest["sha256"]["training_notebook"],
        "model_artifact_sha256": manifest["sha256"]["model_artifact_tree"],
        "policy_threshold": manifest["policy"]["threshold"],
        "policy_max_alerts_per_cutoff": manifest["policy"]["max_alerts_per_cutoff"],
        "policy_cooldown_hours": manifest["policy"]["cooldown_hours"],
        "holdout_released": False,
        "manifest_json": json.dumps(manifest, sort_keys=True),
    }
    return manifest, row


def test_consumes_manifest_written_by_training(freeze):
    manifest, row = freeze

    loaded, digest = parse_training_freeze(
        row, manifest["freeze_id"], manifest["model_name"],
        manifest["feature_set_version"], manifest["feature_names"],
        manifest["sha256"]["feature_registry"],
    )

    assert loaded == manifest
    assert digest == hashlib.sha256(row["manifest_json"].encode()).hexdigest()


@pytest.mark.parametrize("change", ["new_threshold", "feature_order", "other_schema"])
def test_rejects_inconsistent_freeze(freeze, change):
    manifest, row = freeze
    model_name = manifest["model_name"]
    feature_names = manifest["feature_names"]
    if change == "new_threshold":
        manifest["policy"]["threshold"] = row["policy_threshold"] = 0.5
        row["manifest_json"] = json.dumps(manifest, sort_keys=True)
    elif change == "feature_order":
        feature_names = list(reversed(feature_names))
    else:
        model_name = "another_catalog.fleet_ml.dont_go_episode_risk"

    with pytest.raises(ValueError, match="diverge"):
        parse_training_freeze(
            row, manifest["freeze_id"], model_name, manifest["feature_set_version"],
            feature_names, manifest["sha256"]["feature_registry"],
        )
