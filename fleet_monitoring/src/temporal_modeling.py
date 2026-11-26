"""Contratos reutilizáveis para treino e política operacional da v2."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import yaml
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

REQUIRED_FEATURE_FIELDS = {
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


def sha256_file(path: Path) -> str:
    """Calcula SHA-256 em streaming."""
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_tree(path: Path) -> str:
    """Hash determinístico de nomes e conteúdo de todos os arquivos."""
    digest = hashlib.sha256()
    for file_path in sorted(item for item in path.rglob("*") if item.is_file()):
        digest.update(file_path.relative_to(path).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(sha256_file(file_path)))
    return digest.hexdigest()


def load_feature_registry(path: Path) -> tuple[dict[str, Any], list[str]]:
    """Carrega e valida a lista ordenada de features congeladas."""
    registry = yaml.safe_load(path.read_text(encoding="utf-8"))
    features = registry.get("features", [])
    names = [feature.get("name") for feature in features]

    if len(features) != registry.get("feature_count"):
        raise ValueError("feature_count diverge da quantidade registrada")
    if len(names) != len(set(names)):
        raise ValueError("o registro contém nomes de feature duplicados")
    if any(REQUIRED_FEATURE_FIELDS - set(feature) for feature in features):
        raise ValueError("o registro contém feature sem metadados obrigatórios")
    if set(names).intersection(registry.get("prohibited_inputs", [])):
        raise ValueError("o registro contém input explicitamente proibido")
    if registry.get("target") in names:
        raise ValueError("o alvo não pode integrar a matriz de features")
    return registry, names


def to_model_matrix(frame: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    """Seleciona a ordem congelada e converte booleanos em valores numéricos."""
    missing = sorted(set(feature_names).difference(frame.columns))
    if missing:
        raise ValueError(f"features ausentes: {missing}")
    prohibited = {"y_episode", "next_episode_id", "next_episode_start", "tag"}
    if prohibited.intersection(feature_names):
        raise ValueError("identidade ou outcome futuro presente nas features")

    matrix = frame.loc[:, feature_names].copy()
    for column in matrix.columns:
        if pd.api.types.is_bool_dtype(matrix[column].dtype):
            matrix[column] = matrix[column].astype("int8")
        else:
            matrix[column] = pd.to_numeric(matrix[column], errors="raise")
    return matrix


def historical_episode_rate_score(frame: pd.DataFrame) -> np.ndarray:
    """Heurística: taxa histórica convertida em risco Poisson para quatro horas."""
    required = {"episodes_30d", "episode_history_exposure_days"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"colunas ausentes para a heurística: {sorted(missing)}")

    episodes = pd.to_numeric(frame["episodes_30d"], errors="raise").fillna(0.0)
    exposure = pd.to_numeric(
        frame["episode_history_exposure_days"], errors="raise"
    ).fillna(0.0)
    observable_days = exposure.clip(lower=0.0, upper=30.0)
    daily_rate = np.where(observable_days >= 1.0, episodes / observable_days, 0.0)
    score = 1.0 - np.exp(-daily_rate * (4.0 / 24.0))
    return np.asarray(score, dtype="float64")


def validated_probabilities(scores: Iterable[float]) -> np.ndarray:
    """Aceita somente ruído de máquina fora de [0,1] e normaliza a borda."""
    probability = np.asarray(list(scores), dtype="float64")
    if probability.size == 0 or not np.isfinite(probability).all():
        raise ValueError("scores precisam ser finitos e não vazios")
    tolerance = 1e-12
    if probability.min() < -tolerance or probability.max() > 1.0 + tolerance:
        raise ValueError("scores materialmente fora do intervalo [0,1]")
    return np.clip(probability, 0.0, 1.0)


def binary_metrics(y_true: Iterable[int], scores: Iterable[float]) -> dict[str, float]:
    """Métricas independentes de threshold com proteção a classe única."""
    truth = np.asarray(list(y_true), dtype="int8")
    probability = validated_probabilities(scores)
    if truth.size == 0 or probability.size != truth.size:
        raise ValueError("y_true e scores precisam ter o mesmo tamanho não vazio")

    roc_auc = float("nan")
    if np.unique(truth).size == 2:
        roc_auc = float(roc_auc_score(truth, probability))
    return {
        "average_precision": float(average_precision_score(truth, probability)),
        "roc_auc": roc_auc,
        "brier": float(brier_score_loss(truth, probability)),
        "prevalence": float(truth.mean()),
        "row_count": float(truth.size),
        "positive_count": float(truth.sum()),
    }


def threshold_metrics(
    y_true: Iterable[int], scores: Iterable[float], threshold: float
) -> dict[str, float]:
    """Métricas binárias no threshold congelado."""
    truth = np.asarray(list(y_true), dtype="int8")
    probability = validated_probabilities(scores)
    prediction = (probability >= threshold).astype("int8")
    tn, fp, fn, tp = confusion_matrix(truth, prediction, labels=[0, 1]).ravel()
    return {
        "threshold": float(threshold),
        "precision": float(precision_score(truth, prediction, zero_division=0)),
        "recall": float(recall_score(truth, prediction, zero_division=0)),
        "f1": float(f1_score(truth, prediction, zero_division=0)),
        "true_negative": float(tn),
        "false_positive": float(fp),
        "false_negative": float(fn),
        "true_positive": float(tp),
    }


def assert_released_scope(
    frame: pd.DataFrame,
    *,
    exclusive_upper_bound: str | pd.Timestamp = "2025-06-01",
) -> None:
    """Impede que seleção, treino, calibração ou política leiam o holdout."""
    required = {"prediction_timestamp", "is_released", "split_id"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"metadados de liberação ausentes: {sorted(missing)}")
    if frame["split_id"].astype(str).str.contains("holdout", case=False).any():
        raise ValueError("o frame contém split de holdout")
    if not frame["is_released"].astype(bool).all():
        raise ValueError("o frame contém atribuição não liberada")
    timestamps = pd.to_datetime(frame["prediction_timestamp"])
    if (timestamps >= pd.Timestamp(exclusive_upper_bound)).any():
        raise ValueError("o frame alcança ou ultrapassa a fronteira do holdout")


def apply_alert_policy(
    frame: pd.DataFrame,
    *,
    threshold: float,
    max_alerts_per_cutoff: int,
    cooldown_hours: int,
) -> pd.DataFrame:
    """Aplica ranking, capacidade, cooldown e matching um-para-um."""
    required = {
        "scoring_row_id",
        "prediction_timestamp",
        "tag",
        "risk_score",
        "y_episode",
        "next_episode_id",
        "lead_time_minutes",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"colunas ausentes para política: {sorted(missing)}")
    if max_alerts_per_cutoff < 1 or cooldown_hours < 0:
        raise ValueError("capacidade deve ser positiva e cooldown não negativo")

    scored = frame.copy()
    scored["prediction_timestamp"] = pd.to_datetime(scored["prediction_timestamp"])
    scored["risk_score"] = validated_probabilities(scored["risk_score"])
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

    scored["matched_episode_id"] = pd.Series(pd.NA, index=scored.index, dtype="string")
    scored["is_true_alert"] = False
    matched_episodes: set[str] = set()
    emitted = scored.loc[scored["is_emitted_alert"]].sort_values(
        ["prediction_timestamp", "risk_score", "scoring_row_id"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    for row_index, row in emitted.iterrows():
        episode_id = row["next_episode_id"]
        if int(row["y_episode"]) != 1 or pd.isna(episode_id):
            continue
        episode_key = str(episode_id)
        if episode_key in matched_episodes:
            continue
        scored.at[row_index, "matched_episode_id"] = episode_key
        scored.at[row_index, "is_true_alert"] = True
        matched_episodes.add(episode_key)

    return scored.sort_index()


def operational_metrics(scored: pd.DataFrame) -> dict[str, float]:
    """Calcula métricas por alerta e episódio targetável."""
    required = {
        "prediction_timestamp",
        "y_episode",
        "next_episode_id",
        "lead_time_minutes",
        "is_emitted_alert",
        "is_true_alert",
        "matched_episode_id",
    }
    missing = required.difference(scored.columns)
    if missing:
        raise ValueError(f"colunas operacionais ausentes: {sorted(missing)}")

    targetable = {
        str(value)
        for value in scored.loc[
            (scored["y_episode"].astype(int) == 1) & scored["next_episode_id"].notna(),
            "next_episode_id",
        ]
    }
    emitted = scored.loc[scored["is_emitted_alert"].astype(bool)]
    true_alerts = emitted.loc[emitted["is_true_alert"].astype(bool)]
    matched = set(true_alerts["matched_episode_id"].dropna().astype(str))

    alert_count = int(len(emitted))
    true_count = int(len(true_alerts))
    targetable_count = int(len(targetable))
    precision = true_count / alert_count if alert_count else 0.0
    recall = len(matched) / targetable_count if targetable_count else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    active_cutoffs = max(int(scored["prediction_timestamp"].nunique()), 1)
    active_days = max(int(pd.to_datetime(scored["prediction_timestamp"]).dt.date.nunique()), 1)
    lead = pd.to_numeric(true_alerts["lead_time_minutes"], errors="coerce").dropna()

    return {
        "alert_count": float(alert_count),
        "true_alert_count": float(true_count),
        "false_alert_count": float(alert_count - true_count),
        "targetable_episode_count": float(targetable_count),
        "matched_episode_count": float(len(matched)),
        "episode_precision": float(precision),
        "targetable_episode_recall": float(recall),
        "episode_f1": float(f1),
        "median_lead_time_minutes": float(lead.median()) if not lead.empty else float("nan"),
        "alerts_per_active_cutoff": float(alert_count / active_cutoffs),
        "alerts_per_active_day": float(alert_count / active_days),
    }


def select_alert_policy(
    frame: pd.DataFrame,
    *,
    max_alerts_candidates: tuple[int, ...] = (1, 3, 5),
    cooldown_candidates: tuple[int, ...] = (4, 8, 12),
    quantile_count: int = 49,
) -> tuple[dict[str, float], pd.DataFrame]:
    """Escolhe política por F1 de episódio em grade pré-definida de maio."""
    scores = validated_probabilities(frame["risk_score"])
    quantiles = np.linspace(0.02, 0.98, quantile_count)
    thresholds = np.unique(np.quantile(scores, quantiles))

    evaluations: list[dict[str, float]] = []
    for threshold in thresholds:
        for max_alerts in max_alerts_candidates:
            for cooldown_hours in cooldown_candidates:
                applied = apply_alert_policy(
                    frame,
                    threshold=float(threshold),
                    max_alerts_per_cutoff=int(max_alerts),
                    cooldown_hours=int(cooldown_hours),
                )
                metrics = operational_metrics(applied)
                evaluations.append(
                    {
                        "threshold": float(threshold),
                        "max_alerts_per_cutoff": float(max_alerts),
                        "cooldown_hours": float(cooldown_hours),
                        **metrics,
                    }
                )

    grid = pd.DataFrame(evaluations)
    ranked = grid.sort_values(
        [
            "episode_f1",
            "episode_precision",
            "targetable_episode_recall",
            "alert_count",
            "threshold",
            "cooldown_hours",
            "max_alerts_per_cutoff",
        ],
        ascending=[False, False, False, True, False, False, True],
        kind="mergesort",
    )
    return ranked.iloc[0].to_dict(), grid
