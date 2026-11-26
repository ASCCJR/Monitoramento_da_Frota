"""Contratos puros para scoring batch do pacote de modelagem congelado."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd


def scoring_run_id(
    *,
    freeze_id: str,
    model_name: str,
    model_version: str,
    feature_registry_sha256: str,
    policy: dict[str, Any],
    score_start: str,
    score_end: str,
    contract_version: str = "1.0.0",
) -> str:
    """Identifica de forma determinística um lote e sua política."""
    payload = {
        "contract_version": contract_version,
        "feature_registry_sha256": feature_registry_sha256,
        "freeze_id": freeze_id,
        "model_name": model_name,
        "model_version": str(model_version),
        "policy": policy,
        "score_end": pd.Timestamp(score_end).isoformat(),
        "score_start": pd.Timestamp(score_start).isoformat(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


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


def select_scoring_window(
    scored_with_context: pd.DataFrame,
    *,
    score_start: str,
    score_end: str,
) -> pd.DataFrame:
    """Remove o contexto anterior após a política ter sido aplicada."""
    start = pd.Timestamp(score_start)
    end = pd.Timestamp(score_end)
    if start >= end:
        raise ValueError("score_start deve anteceder score_end")
    timestamps = pd.to_datetime(scored_with_context["prediction_timestamp"])
    return scored_with_context.loc[(timestamps >= start) & (timestamps < end)].copy()


def _nested_estimators(model: Any):
    """Percorre wrappers de calibração e pipelines sem depender da versão."""
    queue = [model]
    visited: set[int] = set()
    while queue:
        candidate = queue.pop(0)
        if candidate is None or id(candidate) in visited:
            continue
        visited.add(id(candidate))
        yield candidate
        for attribute in ("estimator", "base_estimator", "classifier"):
            nested = getattr(candidate, attribute, None)
            if nested is not None:
                queue.append(nested)
        named_steps = getattr(candidate, "named_steps", None)
        if named_steps is not None:
            queue.extend(list(named_steps.values()))
        steps = getattr(candidate, "steps", None)
        if steps is not None:
            queue.extend(step_estimator for _, step_estimator in steps)
        for attribute in ("calibrated_classifiers_", "estimators_", "estimators"):
            nested_many = getattr(candidate, attribute, None)
            if nested_many is not None:
                queue.extend(list(nested_many))


def _find_importance_estimator(model: Any) -> Any:
    """Localiza o estimador treinado mesmo entre wrappers sklearn versionados."""
    for candidate in _nested_estimators(model):
        if any(
            hasattr(candidate, attribute)
            for attribute in ("booster_", "feature_importances_", "coef_")
        ):
            return candidate
    raise TypeError("estimador não expõe importância de features")


def extract_feature_importance(
    model: Any, feature_names: Iterable[str]
) -> pd.DataFrame:
    """Extrai ganho LightGBM, preservando a ordem do contrato de features."""
    names = list(feature_names)
    estimator = _find_importance_estimator(model)
    booster = getattr(estimator, "booster_", None)
    if booster is not None:
        values = np.asarray(booster.feature_importance(importance_type="gain"), dtype=float)
    elif hasattr(estimator, "feature_importances_"):
        values = np.asarray(estimator.feature_importances_, dtype=float)
    elif hasattr(estimator, "coef_"):
        values = np.abs(np.asarray(estimator.coef_, dtype=float)).reshape(-1)
    if len(values) > len(names):
        imputer = next(
            (
                candidate
                for candidate in _nested_estimators(model)
                if hasattr(candidate, "indicator_")
            ),
            None,
        )
        indicator = getattr(imputer, "indicator_", None)
        indicator_indices = np.asarray(
            getattr(indicator, "features_", []), dtype=int
        ).reshape(-1)
        if len(values) != len(names) + len(indicator_indices):
            raise ValueError("expansão do imputer diverge das importâncias do modelo")
        aggregated = values[: len(names)].copy()
        for source_index, indicator_gain in zip(
            indicator_indices, values[len(names) :], strict=True
        ):
            aggregated[int(source_index)] += float(indicator_gain)
        values = aggregated
    if len(values) != len(names):
        raise ValueError("quantidade de importâncias diverge do feature registry")
    if not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("importâncias inválidas")
    total = float(values.sum())
    normalized = values / total if total > 0 else np.zeros_like(values)
    result = pd.DataFrame(
        {
            "feature_name": names,
            "importance_gain": values,
            "importance_normalized": normalized,
        }
    )
    result["importance_rank"] = (
        result["importance_gain"].rank(method="first", ascending=False).astype(int)
    )
    return result.sort_values("importance_rank", kind="mergesort").reset_index(drop=True)
