"""Política de avisos extraída de `fleet_monitoring/src/batch_scoring.py` do projeto original."""

from __future__ import annotations

import numpy as np
import pandas as pd


def apply_operational_policy(
    frame: pd.DataFrame,
    *,
    threshold: float,
    max_alerts_per_cutoff: int,
    cooldown_hours: int,
) -> pd.DataFrame:
    """Aplica ranking, capacidade e cooldown sem consultar labels ou outcomes."""
    required = {"scoring_row_id", "prediction_timestamp", "tag", "risk_score"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"colunas ausentes para política: {sorted(missing)}")
    if not 0 <= float(threshold) <= 1:
        raise ValueError("threshold deve estar entre zero e um")
    if max_alerts_per_cutoff < 1 or cooldown_hours < 0:
        raise ValueError("capacidade deve ser positiva e cooldown não negativo")

    scored = frame.copy()
    scored["prediction_timestamp"] = pd.to_datetime(
        scored["prediction_timestamp"], errors="raise"
    )
    probabilities = np.asarray(scored["risk_score"], dtype=float)
    if not np.isfinite(probabilities).all():
        raise ValueError("scores contêm valores não finitos")
    if ((probabilities < -1e-12) | (probabilities > 1 + 1e-12)).any():
        raise ValueError("scores fora do intervalo probabilístico")
    scored["risk_score"] = np.clip(probabilities, 0.0, 1.0)

    scored = scored.sort_values(
        ["prediction_timestamp", "risk_score", "scoring_row_id"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    scored["rank_at_cutoff"] = (
        scored.groupby("prediction_timestamp", sort=False).cumcount() + 1
    )
    scored["is_policy_candidate"] = (
        (scored["risk_score"] >= float(threshold))
        & (scored["rank_at_cutoff"] <= int(max_alerts_per_cutoff))
    )
    scored["is_emitted_alert"] = False

    last_alert_by_tag: dict[str, pd.Timestamp] = {}
    cooldown = pd.Timedelta(hours=int(cooldown_hours))
    candidates = scored.loc[scored["is_policy_candidate"]].sort_values(
        ["prediction_timestamp", "risk_score", "tag", "scoring_row_id"],
        ascending=[True, False, True, True],
        kind="mergesort",
    )
    for row_index, row in candidates.iterrows():
        tag = str(row["tag"])
        timestamp = pd.Timestamp(row["prediction_timestamp"])
        previous = last_alert_by_tag.get(tag)
        if previous is None or timestamp - previous >= cooldown:
            scored.at[row_index, "is_emitted_alert"] = True
            last_alert_by_tag[tag] = timestamp

    return scored.sort_index()
