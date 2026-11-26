from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "fleet_monitoring/manifests/features_v1.yml"


def test_feature_registry_contract_is_complete_and_unique() -> None:
    registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    features = registry["features"]
    names = [feature["name"] for feature in features]
    required = {
        "name",
        "family",
        "data_type",
        "definition",
        "source",
        "event_time",
        "availability_rule",
        "window",
        "null_rule",
        "model_use",
    }

    assert len(features) == registry["feature_count"] == 46
    assert len(names) == len(set(names))
    assert all(required <= set(feature) for feature in features)
    assert {feature["model_use"] for feature in features} == {"primary"}


def test_feature_registry_excludes_prohibited_inputs() -> None:
    registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    feature_names = {feature["name"] for feature in registry["features"]}

    assert feature_names.isdisjoint(registry["prohibited_inputs"])
    assert "month" not in feature_names
    assert not any("operator" in name or "operador" in name for name in feature_names)
    assert not any("cycle" in name or "ciclo" in name for name in feature_names)
