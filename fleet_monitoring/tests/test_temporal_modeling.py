from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from fleet_monitoring.src.temporal_modeling import (
    apply_alert_policy,
    assert_released_scope,
    historical_episode_rate_score,
    load_feature_registry,
    operational_metrics,
    select_alert_policy,
    to_model_matrix,
    validated_probabilities,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "fleet_monitoring/manifests/features_v1.yml"


def test_registry_drives_exact_model_matrix_order() -> None:
    registry, feature_names = load_feature_registry(REGISTRY_PATH)
    row = {name: index for index, name in enumerate(reversed(feature_names))}
    frame = pd.DataFrame([row])

    matrix = to_model_matrix(frame, feature_names)

    assert list(matrix.columns) == feature_names
    assert len(feature_names) == registry["feature_count"] == 46


def test_historical_heuristic_uses_only_observable_past() -> None:
    frame = pd.DataFrame(
        {
            "episodes_30d": [0, 3, 10],
            "episode_history_exposure_days": [0.5, 10.0, 50.0],
        }
    )

    scores = historical_episode_rate_score(frame)

    expected = [
        0.0,
        1.0 - np.exp(-0.3 * (4 / 24)),
        1.0 - np.exp(-(10 / 30) * (4 / 24)),
    ]
    assert np.allclose(scores, expected)


def test_probability_validation_clips_only_machine_precision_noise() -> None:
    probabilities = validated_probabilities([-1e-15, 0.2, 1.0 + 1e-15])

    assert np.allclose(probabilities, [0.0, 0.2, 1.0])
    with pytest.raises(ValueError, match="materialmente"):
        validated_probabilities([1.000001])


def test_released_scope_rejects_holdout_or_june() -> None:
    valid = pd.DataFrame(
        {
            "prediction_timestamp": ["2025-05-30 19:00:00"],
            "is_released": [True],
            "split_id": ["policy_may_16_31"],
        }
    )
    assert_released_scope(valid)

    with pytest.raises(ValueError, match="holdout"):
        assert_released_scope(
            valid.assign(split_id="holdout_june", is_released=False)
        )
    with pytest.raises(ValueError, match="fronteira"):
        assert_released_scope(
            valid.assign(prediction_timestamp="2025-06-01 00:00:00")
        )


def _policy_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"scoring_row_id": "a1", "prediction_timestamp": "2025-05-16 00:00:00", "tag": "A", "risk_score": 0.90, "y_episode": 1, "next_episode_id": "ep1", "lead_time_minutes": 180.0},
            {"scoring_row_id": "b1", "prediction_timestamp": "2025-05-16 00:00:00", "tag": "B", "risk_score": 0.80, "y_episode": 0, "next_episode_id": None, "lead_time_minutes": None},
            {"scoring_row_id": "a2", "prediction_timestamp": "2025-05-16 01:00:00", "tag": "A", "risk_score": 0.95, "y_episode": 1, "next_episode_id": "ep1", "lead_time_minutes": 120.0},
            {"scoring_row_id": "c1", "prediction_timestamp": "2025-05-16 01:00:00", "tag": "C", "risk_score": 0.70, "y_episode": 1, "next_episode_id": "ep2", "lead_time_minutes": 60.0},
        ]
    )


def test_policy_applies_capacity_cooldown_and_one_to_one_matching() -> None:
    applied = apply_alert_policy(
        _policy_frame(),
        threshold=0.65,
        max_alerts_per_cutoff=2,
        cooldown_hours=4,
    )
    metrics = operational_metrics(applied)

    assert applied.set_index("scoring_row_id").loc["a2", "is_emitted_alert"] == 0
    assert metrics["alert_count"] == 3
    assert metrics["matched_episode_count"] == 2
    assert metrics["targetable_episode_count"] == 2
    assert metrics["episode_precision"] == pytest.approx(2 / 3)
    assert metrics["targetable_episode_recall"] == 1.0


def test_policy_selection_is_deterministic() -> None:
    first, first_grid = select_alert_policy(
        _policy_frame(),
        max_alerts_candidates=(1, 2),
        cooldown_candidates=(0, 4),
        quantile_count=5,
    )
    second, second_grid = select_alert_policy(
        _policy_frame(),
        max_alerts_candidates=(1, 2),
        cooldown_candidates=(0, 4),
        quantile_count=5,
    )

    assert first == second
    pd.testing.assert_frame_equal(first_grid, second_grid)
