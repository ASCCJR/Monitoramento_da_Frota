from __future__ import annotations

import pandas as pd
import pytest

from fleet_monitoring.src.batch_scoring import (
    apply_operational_policy,
    extract_feature_importance,
    scoring_run_id,
    select_scoring_window,
)


def test_policy_ranks_then_applies_capacity_and_tag_cooldown() -> None:
    frame = pd.DataFrame(
        [
            {"scoring_row_id": "a0", "prediction_timestamp": "2025-05-31 23:00", "tag": "A", "risk_score": 0.90},
            {"scoring_row_id": "a1", "prediction_timestamp": "2025-06-01 00:00", "tag": "A", "risk_score": 0.80},
            {"scoring_row_id": "b1", "prediction_timestamp": "2025-06-01 00:00", "tag": "B", "risk_score": 0.70},
            {"scoring_row_id": "c1", "prediction_timestamp": "2025-06-01 00:00", "tag": "C", "risk_score": 0.60},
            {"scoring_row_id": "a2", "prediction_timestamp": "2025-06-01 03:00", "tag": "A", "risk_score": 0.95},
            {"scoring_row_id": "a3", "prediction_timestamp": "2025-06-01 04:00", "tag": "A", "risk_score": 0.95},
        ]
    )
    scored = apply_operational_policy(
        frame, threshold=0.5, max_alerts_per_cutoff=2, cooldown_hours=4
    )
    assert scored.set_index("scoring_row_id").loc["c1", "rank_at_cutoff"] == 3
    assert not bool(scored.set_index("scoring_row_id").loc["c1", "is_policy_candidate"])
    assert not bool(scored.set_index("scoring_row_id").loc["a1", "is_emitted_alert"])
    assert bool(scored.set_index("scoring_row_id").loc["a2", "is_emitted_alert"])
    assert not bool(scored.set_index("scoring_row_id").loc["a3", "is_emitted_alert"])


def test_context_is_removed_only_after_policy() -> None:
    scored = pd.DataFrame(
        {
            "prediction_timestamp": pd.to_datetime(
                ["2025-05-31 23:00", "2025-06-01 00:00", "2025-07-01 00:00"]
            ),
            "value": [1, 2, 3],
        }
    )
    result = select_scoring_window(
        scored, score_start="2025-06-01", score_end="2025-07-01"
    )
    assert result["value"].tolist() == [2]


def test_scoring_id_is_deterministic_and_policy_sensitive() -> None:
    kwargs = {
        "freeze_id": "f",
        "model_name": "m",
        "model_version": "1",
        "feature_registry_sha256": "h",
        "policy": {"threshold": 0.2, "max_alerts_per_cutoff": 5, "cooldown_hours": 4},
        "score_start": "2025-06-01",
        "score_end": "2025-07-01",
    }
    assert scoring_run_id(**kwargs) == scoring_run_id(**kwargs)
    changed = {**kwargs, "policy": {**kwargs["policy"], "threshold": 0.3}}
    assert scoring_run_id(**kwargs) != scoring_run_id(**changed)


def test_invalid_probability_fails_materially() -> None:
    frame = pd.DataFrame(
        [{"scoring_row_id": "x", "prediction_timestamp": "2025-06-01", "tag": "A", "risk_score": 1.1}]
    )
    with pytest.raises(ValueError, match="probabilístico"):
        apply_operational_policy(frame, threshold=0.5, max_alerts_per_cutoff=1, cooldown_hours=4)


def test_feature_importance_uses_gain_and_normalizes() -> None:
    class Booster:
        def feature_importance(self, importance_type: str):
            assert importance_type == "gain"
            return [1.0, 3.0, 5.0]

    class Estimator:
        booster_ = Booster()

    class Indicator:
        features_ = [1]

    class Imputer:
        indicator_ = Indicator()

    class Pipeline:
        named_steps = {"imputer": Imputer(), "classifier": Estimator()}

    class FrozenEstimator:
        estimator = Pipeline()

    class Calibrated:
        base_estimator = FrozenEstimator()

    class Model:
        calibrated_classifiers_ = [Calibrated()]

    result = extract_feature_importance(Model(), ["a", "b"])
    assert result["feature_name"].tolist() == ["b", "a"]
    assert result["importance_normalized"].sum() == pytest.approx(1.0)
    assert result.iloc[0]["importance_rank"] == 1
    assert result.iloc[0]["importance_gain"] == pytest.approx(8.0)
