# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # 02 — Treino temporal, MLflow e freeze da política
#
# ## Goal
#
# Selecionar um candidato nos folds expansivos de janeiro a abril, treinar em
# janeiro–abril, calibrar em 1–15/05 e congelar uma política analítica em
# 16–31/05. Este notebook não contém consulta ao holdout de junho.
#
# ## Key assumptions
#
# - a ordem das 46 features vem exclusivamente de `features_v1.yml`;
# - o objetivo de seleção é a média macro da AUC-PR dos três folds;
# - a calibração sigmoidal é pré-definida e não escolhida por resultado;
# - threshold, capacidade e cooldown são escolhidos somente no bloco de
#   política de maio por F1 de episódio targetável;
# - a política é cenário analítico e não uma capacidade confirmada pela Vale;
# - somente o alias `challenger` pode ser movido neste gate.

# %% [markdown]
# ## 1. Setup, parâmetros e hashes

# %%
from __future__ import annotations

import json
import math
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import mlflow
import mlflow.sklearn
import numpy as np
import optuna
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sklearn.calibration import CalibratedClassifierCV
from sklearn.dummy import DummyClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

spark = globals()["spark"]
dbutils = globals()["dbutils"]


PARAMETER_DEFAULTS = {
    "catalog": "",
    "gold_schema": "",
    "ml_schema": "",
    "experiment_path": "",
    "project_files_path": "",
    "optuna_trials": "20",
    "random_seed": "20260824",
}
for parameter_name, default_value in PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(parameter_name, default_value)


def parameter(name: str) -> str:
    return dbutils.widgets.get(name).strip()


CATALOG = parameter("catalog")
GOLD_SCHEMA = parameter("gold_schema")
ML_SCHEMA = parameter("ml_schema")
EXPERIMENT_PATH = parameter("experiment_path")
MODEL_NAME = f"{CATALOG}.{ML_SCHEMA}.dont_go_episode_risk"
project_files = parameter("project_files_path")
PROJECT_FILES_PATH = Path(project_files)
OPTUNA_TRIALS = int(parameter("optuna_trials"))
RANDOM_SEED = int(parameter("random_seed"))

if OPTUNA_TRIALS < 1:
    raise ValueError("optuna_trials precisa ser positivo")
if not project_files or not PROJECT_FILES_PATH.is_dir():
    raise FileNotFoundError(f"bundle files ausente: {PROJECT_FILES_PATH}")
if str(PROJECT_FILES_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_FILES_PATH))

from src.model_freeze import training_freeze_id, validate_ml_namespaces  # noqa: E402
from src.temporal_modeling import (  # noqa: E402
    apply_alert_policy,
    assert_released_scope,
    binary_metrics,
    historical_episode_rate_score,
    load_feature_registry,
    operational_metrics,
    select_alert_policy,
    sha256_file,
    sha256_tree,
    threshold_metrics,
    to_model_matrix,
)

validate_ml_namespaces(CATALOG, GOLD_SCHEMA, ML_SCHEMA)

FEATURE_REGISTRY_PATH = PROJECT_FILES_PATH / "manifests/features_v1.yml"
MODELING_CODE_PATH = PROJECT_FILES_PATH / "src/temporal_modeling.py"
feature_registry_sha256 = sha256_file(FEATURE_REGISTRY_PATH)
modeling_code_sha256 = sha256_file(MODELING_CODE_PATH)
TRAINING_NOTEBOOK_SHA256 = sha256_file(
    PROJECT_FILES_PATH / "notebooks/02_train_and_freeze.py"
)

feature_registry, FEATURE_NAMES = load_feature_registry(FEATURE_REGISTRY_PATH)
if feature_registry["version"] != "1.0.0" or len(FEATURE_NAMES) != 46:
    raise RuntimeError("feature set diferente da versão 1.0.0 congelada")

TRAINING_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_training_set"
ASSIGNMENTS_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_training_assignments"
METRICS_TABLE = f"{CATALOG}.{ML_SCHEMA}.development_fold_metrics"
FREEZE_TABLE = f"{CATALOG}.{ML_SCHEMA}.model_freeze_manifest"
POLICY_SCORES_TABLE = f"{CATALOG}.{ML_SCHEMA}.policy_validation_scores"

print(f"Experimento            : {EXPERIMENT_PATH}")
print(f"Modelo UC              : {MODEL_NAME}")
print(f"Training set           : {TRAINING_TABLE}")
print(f"Feature set            : {feature_registry['version']} ({len(FEATURE_NAMES)})")
print(f"Optuna trials          : {OPTUNA_TRIALS}")
print(f"Registro SHA-256       : {feature_registry_sha256}")
print("Holdout                : BLOQUEADO — nenhuma consulta de junho neste notebook")

# %% [markdown]
# ## 2. Carregar somente atribuições explicitamente liberadas

# %%
CONTEXT_COLUMNS = [
    "scoring_row_id",
    "prediction_timestamp",
    "tag",
    "tipo_equipamento",
    "y_episode",
    "next_episode_id",
    "next_episode_start",
    "lead_time_minutes",
]


def released_split_spark(split_id: str, split_role: str) -> DataFrame:
    """A consulta exige papel exato e `is_released = true`."""
    assignments = (
        spark.table(ASSIGNMENTS_TABLE)
        .where(
            (F.col("split_id") == split_id)
            & (F.col("split_role") == split_role)
            & F.col("is_released")
        )
        .alias("assignment")
    )
    training = spark.table(TRAINING_TABLE).alias("training")
    return assignments.join(training, on="scoring_row_id", how="inner").select(
        F.col("assignment.split_id"),
        F.col("assignment.split_role"),
        F.col("assignment.is_released"),
        F.col("assignment.prediction_timestamp").alias("prediction_timestamp"),
        F.col("training.scoring_row_id"),
        F.col("training.tag"),
        F.col("training.tipo_equipamento"),
        F.col("training.y_episode").cast("int").alias("y_episode"),
        F.col("training.next_episode_id"),
        F.col("training.next_episode_start"),
        F.col("training.lead_time_minutes").cast("double").alias("lead_time_minutes"),
        *[F.col(f"training.{name}").alias(name) for name in FEATURE_NAMES],
    )


def released_split_pandas(split_id: str, split_role: str) -> pd.DataFrame:
    frame = released_split_spark(split_id, split_role).toPandas()
    assert_released_scope(frame)
    if frame.empty:
        raise RuntimeError(f"split vazio: {split_id}/{split_role}")
    if frame["scoring_row_id"].duplicated().any():
        raise RuntimeError(f"chave duplicada: {split_id}/{split_role}")
    return frame


DEVELOPMENT_FOLDS = ("fold_1", "fold_2", "fold_3")
development: dict[str, dict[str, pd.DataFrame]] = {}
for fold_id in DEVELOPMENT_FOLDS:
    development[fold_id] = {
        "train": released_split_pandas(fold_id, "train"),
        "validation": released_split_pandas(fold_id, "validation"),
    }
    print(
        f"{fold_id}: train={len(development[fold_id]['train']):,} | "
        f"validation={len(development[fold_id]['validation']):,}"
    )

# %% [markdown]
# ## 3. Baselines comparáveis nos três folds

# %%
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.autolog(disable=True)


def make_logistic_model() -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    C=1.0,
                    class_weight=None,
                    max_iter=2000,
                    random_state=RANDOM_SEED,
                    solver="lbfgs",
                ),
            ),
        ]
    )


def lightgbm_parameters(trial: optuna.Trial) -> dict[str, int | float]:
    return {
        "n_estimators": trial.suggest_int("n_estimators", 200, 800),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 16, 96),
        "max_depth": trial.suggest_int("max_depth", 3, 12),
        "min_child_samples": trial.suggest_int("min_child_samples", 20, 240),
        "subsample": trial.suggest_float("subsample", 0.7, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.7, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
    }


def make_lightgbm_model(
    parameters: dict[str, int | float], y_train: pd.Series
) -> Pipeline:
    positives = int(pd.Series(y_train).sum())
    negatives = int(len(y_train) - positives)
    if positives == 0:
        raise ValueError("LightGBM recebeu treino sem classe positiva")
    classifier = lgb.LGBMClassifier(
        objective="binary",
        random_state=RANDOM_SEED,
        n_jobs=-1,
        verbosity=-1,
        deterministic=True,
        force_col_wise=True,
        subsample_freq=1,
        scale_pos_weight=negatives / positives,
        **parameters,
    )
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
            ("classifier", classifier),
        ]
    )


development_metric_rows: list[dict[str, str | float]] = []


def record_metrics(model_name: str, fold_id: str, values: dict[str, float]) -> None:
    for metric_name, metric_value in values.items():
        if math.isfinite(metric_value):
            development_metric_rows.append(
                {
                    "model_name": model_name,
                    "split_id": fold_id,
                    "metric_name": metric_name,
                    "metric_value": float(metric_value),
                }
            )


def macro_average(fold_metrics: list[dict[str, float]]) -> dict[str, float]:
    return {
        metric_name: float(np.nanmean([row[metric_name] for row in fold_metrics]))
        for metric_name in ("average_precision", "roc_auc", "brier")
    }


model_macro_metrics: dict[str, dict[str, float]] = {}
with mlflow.start_run(run_name="development_selection_v1") as selection_run:
    selection_run_id = selection_run.info.run_id
    mlflow.set_tags(
        {
            "stage": "development_selection",
            "feature_set_version": feature_registry["version"],
            "holdout_status": "blocked",
            "selection_metric": "macro_average_precision",
        }
    )
    mlflow.log_params(
        {
            "feature_count": len(FEATURE_NAMES),
            "fold_count": len(DEVELOPMENT_FOLDS),
            "target_horizon_hours": 4,
            "training_notebook_sha256": TRAINING_NOTEBOOK_SHA256,
            "modeling_code_sha256": modeling_code_sha256,
            "feature_registry_sha256": feature_registry_sha256,
        }
    )
    mlflow.log_artifact(str(FEATURE_REGISTRY_PATH), artifact_path="contracts")
    mlflow.log_artifact(str(MODELING_CODE_PATH), artifact_path="code")

    for baseline_name in ("prevalence", "historical_episode_rate", "logistic"):
        fold_metrics: list[dict[str, float]] = []
        with mlflow.start_run(run_name=baseline_name, nested=True):
            for fold_id in DEVELOPMENT_FOLDS:
                train_frame = development[fold_id]["train"]
                validation_frame = development[fold_id]["validation"]
                y_train = train_frame["y_episode"].astype("int8")
                y_validation = validation_frame["y_episode"].astype("int8")

                if baseline_name == "prevalence":
                    dummy = DummyClassifier(strategy="prior")
                    dummy.fit(np.zeros((len(y_train), 1)), y_train)
                    scores = dummy.predict_proba(
                        np.zeros((len(y_validation), 1))
                    )[:, 1]
                elif baseline_name == "historical_episode_rate":
                    scores = historical_episode_rate_score(validation_frame)
                else:
                    logistic = make_logistic_model()
                    logistic.fit(to_model_matrix(train_frame, FEATURE_NAMES), y_train)
                    scores = logistic.predict_proba(
                        to_model_matrix(validation_frame, FEATURE_NAMES)
                    )[:, 1]

                metrics = binary_metrics(y_validation, scores)
                fold_metrics.append(metrics)
                record_metrics(baseline_name, fold_id, metrics)
                for metric_name, metric_value in metrics.items():
                    if math.isfinite(metric_value):
                        mlflow.log_metric(f"{fold_id}_{metric_name}", metric_value)

            macro = macro_average(fold_metrics)
            model_macro_metrics[baseline_name] = macro
            for metric_name, metric_value in macro.items():
                mlflow.log_metric(f"macro_{metric_name}", metric_value)
                record_metrics(baseline_name, "macro", {metric_name: metric_value})

    # Optuna enxerga apenas os mesmos folds janeiro–abril.
    def objective(trial: optuna.Trial) -> float:
        parameters = lightgbm_parameters(trial)
        trial_fold_metrics: list[dict[str, float]] = []
        with mlflow.start_run(
            run_name=f"lightgbm_trial_{trial.number:03d}", nested=True
        ):
            mlflow.log_params(parameters)
            for fold_id in DEVELOPMENT_FOLDS:
                train_frame = development[fold_id]["train"]
                validation_frame = development[fold_id]["validation"]
                y_train = train_frame["y_episode"].astype("int8")
                y_validation = validation_frame["y_episode"].astype("int8")
                model = make_lightgbm_model(parameters, y_train)
                model.fit(to_model_matrix(train_frame, FEATURE_NAMES), y_train)
                scores = model.predict_proba(
                    to_model_matrix(validation_frame, FEATURE_NAMES)
                )[:, 1]
                metrics = binary_metrics(y_validation, scores)
                trial_fold_metrics.append(metrics)
                for metric_name, metric_value in metrics.items():
                    if math.isfinite(metric_value):
                        mlflow.log_metric(f"{fold_id}_{metric_name}", metric_value)

            macro = macro_average(trial_fold_metrics)
            for metric_name, metric_value in macro.items():
                mlflow.log_metric(f"macro_{metric_name}", metric_value)
            trial.set_user_attr("fold_metrics", trial_fold_metrics)
            return macro["average_precision"]

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=RANDOM_SEED),
        study_name="lightgbm_temporal_v1",
    )
    study.optimize(objective, n_trials=OPTUNA_TRIALS, show_progress_bar=False)
    best_lightgbm_fold_metrics = study.best_trial.user_attrs["fold_metrics"]
    model_macro_metrics["lightgbm"] = macro_average(best_lightgbm_fold_metrics)
    for fold_id, metrics in zip(DEVELOPMENT_FOLDS, best_lightgbm_fold_metrics):
        record_metrics("lightgbm", fold_id, metrics)
    for metric_name, metric_value in model_macro_metrics["lightgbm"].items():
        record_metrics("lightgbm", "macro", {metric_name: metric_value})
        mlflow.log_metric(f"best_lightgbm_macro_{metric_name}", metric_value)
    mlflow.log_params(
        {f"best_lightgbm_{name}": value for name, value in study.best_params.items()}
    )

    static_baseline_ap = max(
        model_macro_metrics["prevalence"]["average_precision"],
        model_macro_metrics["historical_episode_rate"]["average_precision"],
    )
    trainable_candidates = {
        "logistic": model_macro_metrics["logistic"]["average_precision"],
        "lightgbm": model_macro_metrics["lightgbm"]["average_precision"],
    }
    selected_algorithm = max(trainable_candidates, key=trainable_candidates.get)
    selected_macro_ap = trainable_candidates[selected_algorithm]
    if selected_macro_ap < static_baseline_ap:
        raise RuntimeError(
            "nenhum modelo treinável superou o melhor baseline estático nos folds"
        )
    mlflow.log_param("selected_algorithm", selected_algorithm)
    mlflow.log_metric("selected_macro_average_precision", selected_macro_ap)

print(f"Selection run          : {selection_run_id}")
print(f"Candidato              : {selected_algorithm}")
print(f"Macro AUC-PR           : {selected_macro_ap:.6f}")

# %% [markdown]
# ## 4. Treinar em janeiro–abril e calibrar em 1–15/05
#
# Maio só é carregado depois que algoritmo e hiperparâmetros estão definidos.

# %%
final_train_spark = released_split_spark("final_train_jan_apr", "train")
calibration_spark = released_split_spark("calibration_may_01_15", "calibration")
final_train = final_train_spark.toPandas()
calibration = calibration_spark.toPandas()
assert_released_scope(final_train)
assert_released_scope(calibration)

X_final_train = to_model_matrix(final_train, FEATURE_NAMES)
y_final_train = final_train["y_episode"].astype("int8")
X_calibration = to_model_matrix(calibration, FEATURE_NAMES)
y_calibration = calibration["y_episode"].astype("int8")

if selected_algorithm == "lightgbm":
    frozen_base_model = make_lightgbm_model(study.best_params, y_final_train)
    frozen_parameters = dict(study.best_params)
else:
    frozen_base_model = make_logistic_model()
    frozen_parameters = {
        "C": 1.0,
        "class_weight": "none",
        "max_iter": 2000,
        "solver": "lbfgs",
    }

# %% [markdown]
# ## 5. Congelar política somente em 16–31/05

# %%
client = MlflowClient(registry_uri="databricks-uc")
freeze_started_at = datetime.now(timezone.utc)

with mlflow.start_run(run_name="frozen_candidate_v1") as final_run:
    final_run_id = final_run.info.run_id
    mlflow.set_tags(
        {
            "stage": "frozen_candidate",
            "feature_set_version": feature_registry["version"],
            "holdout_status": "blocked",
            "model_alias": "challenger",
            "policy_status": "analytical_not_operationally_confirmed",
        }
    )
    mlflow.log_params(
        {
            "selected_algorithm": selected_algorithm,
            "selection_run_id": selection_run_id,
            "feature_count": len(FEATURE_NAMES),
            "feature_set_version": feature_registry["version"],
            "calibration_method": "sigmoid_prefit",
            "target_horizon_hours": 4,
            "policy_selection_objective": "targetable_episode_f1",
            "policy_max_alerts_grid": "1,3,5",
            "policy_cooldown_hours_grid": "4,8,12",
            "policy_threshold_quantiles": 49,
            "training_notebook_sha256": TRAINING_NOTEBOOK_SHA256,
            "modeling_code_sha256": modeling_code_sha256,
            "feature_registry_sha256": feature_registry_sha256,
            **{f"model_{name}": value for name, value in frozen_parameters.items()},
        }
    )
    mlflow.log_artifact(str(FEATURE_REGISTRY_PATH), artifact_path="contracts")
    mlflow.log_artifact(str(MODELING_CODE_PATH), artifact_path="code")

    # Autolog é restrito ao único fit final para não registrar uma versão por trial.
    if selected_algorithm == "lightgbm":
        mlflow.lightgbm.autolog(log_models=False, silent=True)
    else:
        mlflow.sklearn.autolog(log_models=False, silent=True)
    frozen_base_model.fit(X_final_train, y_final_train)
    mlflow.autolog(disable=True)

    raw_calibration_scores = frozen_base_model.predict_proba(X_calibration)[:, 1]
    raw_calibration_metrics = binary_metrics(
        y_calibration, raw_calibration_scores
    )
    calibrated_model = CalibratedClassifierCV(
        estimator=frozen_base_model,
        method="sigmoid",
        cv="prefit",
    )
    calibrated_model.fit(X_calibration, y_calibration)
    calibrated_scores = calibrated_model.predict_proba(X_calibration)[:, 1]
    calibration_metrics = binary_metrics(y_calibration, calibrated_scores)
    for metric_name, metric_value in raw_calibration_metrics.items():
        if math.isfinite(metric_value):
            mlflow.log_metric(f"calibration_raw_{metric_name}", metric_value)
    for metric_name, metric_value in calibration_metrics.items():
        if math.isfinite(metric_value):
            mlflow.log_metric(f"calibration_sigmoid_{metric_name}", metric_value)

    # A política é consultada apenas após o calibrador estar ajustado.
    policy_spark = released_split_spark("policy_may_16_31", "policy")
    policy = policy_spark.toPandas()
    assert_released_scope(policy)
    policy["risk_score"] = calibrated_model.predict_proba(
        to_model_matrix(policy, FEATURE_NAMES)
    )[:, 1]
    selected_policy, policy_grid = select_alert_policy(policy)
    frozen_policy_scores = apply_alert_policy(
        policy,
        threshold=float(selected_policy["threshold"]),
        max_alerts_per_cutoff=int(selected_policy["max_alerts_per_cutoff"]),
        cooldown_hours=int(selected_policy["cooldown_hours"]),
    )
    policy_operational_metrics = operational_metrics(frozen_policy_scores)
    policy_observation_metrics = {
        **binary_metrics(policy["y_episode"], policy["risk_score"]),
        **threshold_metrics(
            policy["y_episode"],
            policy["risk_score"],
            float(selected_policy["threshold"]),
        ),
    }
    mlflow.log_params(
        {
            "policy_threshold": float(selected_policy["threshold"]),
            "policy_max_alerts_per_cutoff": int(
                selected_policy["max_alerts_per_cutoff"]
            ),
            "policy_cooldown_hours": int(selected_policy["cooldown_hours"]),
        }
    )
    for metric_name, metric_value in policy_operational_metrics.items():
        if math.isfinite(metric_value):
            mlflow.log_metric(f"policy_operational_{metric_name}", metric_value)
    for metric_name, metric_value in policy_observation_metrics.items():
        if math.isfinite(metric_value):
            mlflow.log_metric(f"policy_observation_{metric_name}", metric_value)

    # Rastrear exatamente as três populações usadas; nenhuma contém junho.
    mlflow.log_input(
        mlflow.data.from_pandas(
            final_train[FEATURE_NAMES + ["y_episode"]],
            source=TRAINING_TABLE,
            name="final_train_jan_apr",
        ),
        context="training",
    )
    mlflow.log_input(
        mlflow.data.from_pandas(
            calibration[FEATURE_NAMES + ["y_episode"]],
            source=TRAINING_TABLE,
            name="calibration_may_01_15",
        ),
        context="calibration",
    )
    mlflow.log_input(
        mlflow.data.from_pandas(
            policy[FEATURE_NAMES + ["y_episode"]],
            source=TRAINING_TABLE,
            name="policy_may_16_31",
        ),
        context="policy_selection",
    )

    with tempfile.TemporaryDirectory(prefix="vale-v2-policy-") as temporary_dir:
        policy_grid_path = Path(temporary_dir) / "policy_grid_may_v1.csv"
        policy_grid.to_csv(policy_grid_path, index=False)
        mlflow.log_artifact(str(policy_grid_path), artifact_path="policy")

    input_example = X_calibration.head(5)
    signature = infer_signature(
        input_example,
        calibrated_model.predict_proba(input_example)[:, 1],
    )
    model_info = mlflow.sklearn.log_model(
        sk_model=calibrated_model,
        name="model",
        signature=signature,
        input_example=input_example,
        registered_model_name=MODEL_NAME,
        pip_requirements=[
            "mlflow==3.1.0",
            "scikit-learn==1.5.2",
            "lightgbm==4.6.0",
            "pandas==2.1.4",
            "numpy==1.26.4",
        ],
    )
    model_version = str(model_info.registered_model_version)
    client.set_registered_model_alias(MODEL_NAME, "challenger", model_version)

    model_uri = f"models:/{MODEL_NAME}/{model_version}"
    if client.get_model_version(MODEL_NAME, model_version).run_id != final_run_id:
        raise RuntimeError("A versão registrada não aponta para o run deste treino.")
    with tempfile.TemporaryDirectory(prefix="frota-model-freeze-") as temporary_dir:
        downloaded_model = Path(
            mlflow.artifacts.download_artifacts(
                artifact_uri=model_uri,
                dst_path=temporary_dir,
            )
        )
        model_artifact_sha256 = sha256_tree(downloaded_model)
    freeze_manifest = {
        "manifest_version": "1.0.0",
        "frozen_at_utc": freeze_started_at.isoformat(),
        "holdout_released": False,
        "holdout_predictions_viewed": False,
        "model_name": MODEL_NAME,
        "model_version": model_version,
        "model_uri": model_uri,
        "model_alias": "challenger",
        "selection_run_id": selection_run_id,
        "final_run_id": final_run_id,
        "selected_algorithm": selected_algorithm,
        "selected_macro_average_precision": selected_macro_ap,
        "feature_set_version": feature_registry["version"],
        "feature_count": len(FEATURE_NAMES),
        "feature_names": FEATURE_NAMES,
        "calibration_method": "sigmoid_prefit",
        "policy": {
            "threshold": float(selected_policy["threshold"]),
            "max_alerts_per_cutoff": int(
                selected_policy["max_alerts_per_cutoff"]
            ),
            "cooldown_hours": int(selected_policy["cooldown_hours"]),
            "target_horizon_hours": 4,
            "status": "analytical_not_operationally_confirmed",
        },
        "calibration_metrics": calibration_metrics,
        "policy_observation_metrics": policy_observation_metrics,
        "policy_operational_metrics": policy_operational_metrics,
        "sha256": {
            "feature_registry": feature_registry_sha256,
            "modeling_code": modeling_code_sha256,
            "training_notebook": TRAINING_NOTEBOOK_SHA256,
            "model_artifact_tree": model_artifact_sha256,
        },
        "holdout": {
            "split_id": "holdout_june",
            "expected_row_count": 17203,
            "previous_aggregate_label_exposure": True,
        },
    }
    freeze_id = training_freeze_id(freeze_manifest)
    freeze_manifest["freeze_id"] = freeze_id
    freeze_manifest_json = json.dumps(freeze_manifest, sort_keys=True)
    mlflow.log_text(freeze_manifest_json, "freeze/model_freeze.json")

    # Métricas longas, manifesto e scores de maio ficam governados em Delta.
    metric_records = [
        {
            "freeze_id": freeze_id,
            "selection_run_id": selection_run_id,
            **row,
        }
        for row in development_metric_rows
    ]
    spark.createDataFrame(pd.DataFrame(metric_records)).write.mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(METRICS_TABLE)

    freeze_row = spark.createDataFrame(
        [
            {
                "freeze_id": freeze_id,
                "frozen_at_utc": freeze_started_at,
                "holdout_released": False,
                "model_name": MODEL_NAME,
                "model_version": model_version,
                "model_alias": "challenger",
                "selection_run_id": selection_run_id,
                "final_run_id": final_run_id,
                "selected_algorithm": selected_algorithm,
                "feature_set_version": feature_registry["version"],
                "feature_registry_sha256": feature_registry_sha256,
                "modeling_code_sha256": modeling_code_sha256,
                "training_notebook_sha256": TRAINING_NOTEBOOK_SHA256,
                "model_artifact_sha256": model_artifact_sha256,
                "policy_threshold": float(selected_policy["threshold"]),
                "policy_max_alerts_per_cutoff": int(
                    selected_policy["max_alerts_per_cutoff"]
                ),
                "policy_cooldown_hours": int(selected_policy["cooldown_hours"]),
                "manifest_json": freeze_manifest_json,
            }
        ]
    )

    policy_output_columns = [
        "scoring_row_id",
        "prediction_timestamp",
        "tag",
        "tipo_equipamento",
        "y_episode",
        "next_episode_id",
        "next_episode_start",
        "lead_time_minutes",
        "risk_score",
        "rank_at_cutoff",
        "is_policy_candidate",
        "is_emitted_alert",
        "matched_episode_id",
        "is_true_alert",
    ]
    policy_output = frozen_policy_scores[policy_output_columns].copy()
    policy_output["next_episode_id"] = policy_output["next_episode_id"].where(
        policy_output["next_episode_id"].notna(), None
    )
    policy_output["matched_episode_id"] = policy_output["matched_episode_id"].where(
        policy_output["matched_episode_id"].notna(), None
    )
    spark.createDataFrame(policy_output).withColumn(
        "freeze_id", F.lit(freeze_id)
    ).withColumn("model_version", F.lit(model_version)).write.mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(POLICY_SCORES_TABLE)
    freeze_row.write.mode("append").option("mergeSchema", "true").saveAsTable(FREEZE_TABLE)

result = {
    "status": "SUCCESS",
    "freeze_id": freeze_id,
    "selection_run_id": selection_run_id,
    "final_run_id": final_run_id,
    "model_name": MODEL_NAME,
    "model_version": model_version,
    "model_alias": "challenger",
    "selected_algorithm": selected_algorithm,
    "selected_macro_average_precision": selected_macro_ap,
    "policy_threshold": float(selected_policy["threshold"]),
    "policy_max_alerts_per_cutoff": int(selected_policy["max_alerts_per_cutoff"]),
    "policy_cooldown_hours": int(selected_policy["cooldown_hours"]),
    "policy_episode_precision": policy_operational_metrics["episode_precision"],
    "policy_targetable_episode_recall": policy_operational_metrics[
        "targetable_episode_recall"
    ],
    "holdout_released": False,
}
print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
