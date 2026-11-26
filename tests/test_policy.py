import pandas as pd
import pytest

from fleet_monitoring.policy import apply_operational_policy


def test_policy_ranks_then_applies_capacity_and_tag_cooldown() -> None:
    frame = pd.DataFrame(
        [
            ("a0", "2025-05-31 23:00", "A", 0.90),
            ("a1", "2025-06-01 00:00", "A", 0.80),
            ("b1", "2025-06-01 00:00", "B", 0.70),
            ("c1", "2025-06-01 00:00", "C", 0.60),
            ("a2", "2025-06-01 03:00", "A", 0.95),
            ("a3", "2025-06-01 04:00", "A", 0.95),
        ],
        columns=["scoring_row_id", "prediction_timestamp", "tag", "risk_score"],
    )
    scored = apply_operational_policy(
        frame, threshold=0.5, max_alerts_per_cutoff=2, cooldown_hours=4
    ).set_index("scoring_row_id")
    assert scored.loc["c1", "rank_at_cutoff"] == 3
    assert not scored.loc["c1", "is_policy_candidate"]
    assert scored.loc["a1", "is_policy_candidate"]
    assert not scored.loc["a1", "is_emitted_alert"]
    assert scored.loc["a2", "is_emitted_alert"]
    assert not scored.loc["a3", "is_emitted_alert"]


def test_invalid_probability_fails_materially() -> None:
    frame = pd.DataFrame(
        {
            "scoring_row_id": ["x"],
            "prediction_timestamp": ["2025-06-01"],
            "tag": ["A"],
            "risk_score": [1.1],
        }
    )
    with pytest.raises(ValueError, match="probabilístico"):
        apply_operational_policy(
            frame, threshold=0.5, max_alerts_per_cutoff=1, cooldown_hours=4
        )
