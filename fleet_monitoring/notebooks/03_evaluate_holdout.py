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
# # 03 — Liberação controlada e avaliação única do holdout
#
# ## Goal
#
# Avaliar em junho o modelo, calibrador e política do freeze informado. O
# notebook exige versão exata do modelo, registra uma liberação idempotente e
# não possui tuning, seleção de threshold, recalibração ou promoção de alias.
#
# ## Key assumptions
#
# - junho é um holdout processual parcialmente exposto por agregados de labels;
# - o modelo é carregado por versão imutável, nunca por `@challenger`;
# - a política vem do manifesto de treino persistido antes do holdout;
# - métricas por alerta usam matching um-para-um e cooldown já congelado;
# - timestamps de negócio permanecem `TIMESTAMP_NTZ` na saída Gold.

# %% [markdown]
# ## 1. Setup e verificação do freeze

# %%
from __future__ import annotations

import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark = globals()["spark"]
dbutils = globals()["dbutils"]

PARAMETER_DEFAULTS = {
    "catalog": "",
    "silver_schema": "",
    "gold_schema": "",
    "ml_schema": "",
    "experiment_path": "",
    "project_files_path": "",
    "freeze_id": "",
    "job_run_id": "UNKNOWN",
    "task_run_id": "UNKNOWN",
    "release_holdout": "false",
}
for parameter_name, default_value in PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(parameter_name, default_value)


def parameter(name: str) -> str:
    return dbutils.widgets.get(name).strip()


CATALOG = parameter("catalog")
SILVER_SCHEMA = parameter("silver_schema")
GOLD_SCHEMA = parameter("gold_schema")
ML_SCHEMA = parameter("ml_schema")
EXPERIMENT_PATH = parameter("experiment_path")
project_files = parameter("project_files_path")
PROJECT_FILES_PATH = Path(project_files)
FREEZE_ID = parameter("freeze_id")
JOB_RUN_ID = parameter("job_run_id")
TASK_RUN_ID = parameter("task_run_id")
RELEASE_HOLDOUT = parameter("release_holdout").lower() == "true"

if not RELEASE_HOLDOUT:
    raise PermissionError("Use release_holdout=true para liberar a avaliação de junho.")
if not project_files or not PROJECT_FILES_PATH.is_dir():
    raise FileNotFoundError(f"Arquivos do bundle ausentes: {PROJECT_FILES_PATH}")
if str(PROJECT_FILES_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_FILES_PATH))

from src.model_freeze import (  # noqa: E402
    load_frozen_estimator,
    read_training_freeze,
    validate_ml_namespaces,
)
from src.temporal_modeling import (  # noqa: E402
    apply_alert_policy,
    binary_metrics,
    load_feature_registry,
    operational_metrics,
    sha256_file,
    threshold_metrics,
    to_model_matrix,
    validated_probabilities,
)

validate_ml_namespaces(CATALOG, SILVER_SCHEMA, GOLD_SCHEMA, ML_SCHEMA)
FEATURE_REGISTRY_PATH = PROJECT_FILES_PATH / "manifests/features_v1.yml"
feature_registry, FEATURE_NAMES = load_feature_registry(FEATURE_REGISTRY_PATH)
feature_registry_sha256 = sha256_file(FEATURE_REGISTRY_PATH)
EVALUATION_NOTEBOOK_SHA256 = sha256_file(
    PROJECT_FILES_PATH / "notebooks/03_evaluate_holdout.py"
)

MODEL_NAME = f"{CATALOG}.{ML_SCHEMA}.dont_go_episode_risk"

TRAINING_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_training_set"
ASSIGNMENTS_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_training_assignments"
EPISODES_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.int_dontgo_episodios"
FREEZE_TABLE = f"{CATALOG}.{ML_SCHEMA}.model_freeze_manifest"
RELEASE_TABLE = f"{CATALOG}.{ML_SCHEMA}.holdout_release_manifest"
EVALUATION_MANIFEST_TABLE = f"{CATALOG}.{ML_SCHEMA}.holdout_evaluation_manifest"
EVALUATION_METRICS_TABLE = f"{CATALOG}.{ML_SCHEMA}.holdout_evaluation_metrics"
HOLDOUT_SCORES_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_holdout_evaluation_scores"

freeze_manifest, freeze_manifest_sha256, freeze_manifest_json = read_training_freeze(
    spark,
    FREEZE_TABLE,
    FREEZE_ID,
    MODEL_NAME,
    feature_registry["version"],
    FEATURE_NAMES,
    feature_registry_sha256,
)
MODEL_VERSION = freeze_manifest["model_version"]
MODEL_URI = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
POLICY = freeze_manifest["policy"]

release_id = hashlib.sha256(
    json.dumps(
        {
            "freeze_id": FREEZE_ID,
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "split_id": freeze_manifest["holdout"]["split_id"],
            "release_contract_version": "1.0.0",
        },
        sort_keys=True,
    ).encode("utf-8")
).hexdigest()
evaluation_id = hashlib.sha256(
    f"{release_id}|single_holdout_evaluation_v1".encode("utf-8")
).hexdigest()

print(f"Freeze ID              : {FREEZE_ID}")
print(f"Modelo imutável        : {MODEL_URI}")
print(f"Release ID             : {release_id}")
print(f"Evaluation ID          : {evaluation_id}")
print(f"Política               : {POLICY}")

# %% [markdown]
# ## 2. Registrar a liberação antes da primeira consulta a junho

# %%
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.autolog(disable=True)
frozen_model = load_frozen_estimator(freeze_manifest)


def merge_delta(source: DataFrame, table_name: str, keys: list[str]) -> None:
    """Upsert idempotente limitado às chaves determinísticas do artefato."""
    if not spark.catalog.tableExists(table_name):
        source.write.saveAsTable(table_name)
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(source.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


with mlflow.start_run(run_name="holdout_evaluation_v1") as evaluation_run:
    evaluation_run_id = evaluation_run.info.run_id
    mlflow.set_tags(
        {
            "stage": "single_holdout_evaluation",
            "freeze_id": FREEZE_ID,
            "holdout_status": "released_for_single_evaluation",
            "model_version": MODEL_VERSION,
            "policy_changed_after_freeze": "false",
        }
    )
    mlflow.log_params(
        {
            "release_id": release_id,
            "evaluation_id": evaluation_id,
            "model_uri": MODEL_URI,
            "feature_count": len(FEATURE_NAMES),
            "feature_set_version": feature_registry["version"],
            "freeze_manifest_sha256": freeze_manifest_sha256,
            "evaluation_notebook_sha256": EVALUATION_NOTEBOOK_SHA256,
            "policy_threshold": float(POLICY["threshold"]),
            "policy_max_alerts_per_cutoff": int(POLICY["max_alerts_per_cutoff"]),
            "policy_cooldown_hours": int(POLICY["cooldown_hours"]),
        }
    )
    mlflow.log_text(freeze_manifest_json, "contracts/model_freeze.json")
    mlflow.log_artifact(str(FEATURE_REGISTRY_PATH), artifact_path="contracts")

    release_time = datetime.now(timezone.utc)
    current_user = spark.sql("select current_user() as user").first()["user"]
    release_row = spark.createDataFrame(
        [
            {
                "release_id": release_id,
                "release_contract_version": "1.0.0",
                "freeze_id": FREEZE_ID,
                "released_at_utc": release_time,
                "released_by": current_user,
                "release_scope": "holdout_june_single_evaluation",
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "policy_threshold": float(POLICY["threshold"]),
                "policy_max_alerts_per_cutoff": int(POLICY["max_alerts_per_cutoff"]),
                "policy_cooldown_hours": int(POLICY["cooldown_hours"]),
                "job_run_id": JOB_RUN_ID,
                "task_run_id": TASK_RUN_ID,
                "evaluation_run_id": evaluation_run_id,
                "aggregate_labels_previously_exposed": True,
                "predictions_viewed_before_release": False,
            }
        ]
    )
    merge_delta(release_row, RELEASE_TABLE, ["release_id"])
    persisted_release = spark.table(RELEASE_TABLE).where(
        F.col("release_id") == release_id
    )
    if persisted_release.count() != 1:
        raise RuntimeError("liberação não foi persistida de forma única")

    # Primeira consulta ao holdout: só ocorre após o release persistido.
    assignments = (
        spark.table(ASSIGNMENTS_TABLE)
        .where(
            (F.col("split_id") == freeze_manifest["holdout"]["split_id"])
            & (F.col("split_role") == "holdout")
            & F.col("is_holdout")
            & (~F.col("is_released"))
        )
        .alias("assignment")
    )
    training = spark.table(TRAINING_TABLE).alias("training")
    holdout_spark = assignments.join(
        training, on="scoring_row_id", how="inner"
    ).select(
        F.col("assignment.split_id"),
        F.col("assignment.split_role"),
        F.col("assignment.is_holdout"),
        F.col("assignment.is_released"),
        F.col("assignment.label_window_end"),
        F.col("assignment.prediction_timestamp").alias("prediction_timestamp"),
        F.col("training.scoring_row_id"),
        F.col("training.tag"),
        F.col("training.tipo_equipamento"),
        F.col("training.y_episode").cast("int").alias("y_episode"),
        F.col("training.next_episode_id"),
        F.col("training.next_episode_start"),
        F.col("training.lead_time_minutes").cast("double").alias("lead_time_minutes"),
        F.col("training.max_feature_event_time_24h"),
        F.col("training.max_feature_availability_time_24h"),
        *[F.col(f"training.{name}").alias(name) for name in FEATURE_NAMES],
    )
    holdout_row_count = holdout_spark.count()
    if holdout_row_count != int(freeze_manifest["holdout"]["expected_row_count"]):
        raise RuntimeError("cardinalidade do holdout diverge do freeze")
    temporal_violations = holdout_spark.where(
        (F.col("prediction_timestamp") < F.lit("2025-06-01").cast("timestamp_ntz"))
        | (F.col("prediction_timestamp") >= F.lit("2025-07-01").cast("timestamp_ntz"))
        | (F.col("label_window_end") > F.lit("2025-07-01").cast("timestamp_ntz"))
        | (F.col("max_feature_event_time_24h") >= F.col("prediction_timestamp"))
        | (
            F.col("max_feature_availability_time_24h")
            > F.col("prediction_timestamp")
        )
    ).count()
    if temporal_violations:
        raise RuntimeError(f"violações temporais no holdout: {temporal_violations}")

    holdout = holdout_spark.toPandas()
    if holdout["scoring_row_id"].duplicated().any():
        raise RuntimeError("holdout contém scoring_row_id duplicado")

    holdout["risk_score"] = validated_probabilities(
        frozen_model.predict_proba(to_model_matrix(holdout, FEATURE_NAMES))[:, 1]
    )
    evaluated = apply_alert_policy(
        holdout,
        threshold=float(POLICY["threshold"]),
        max_alerts_per_cutoff=int(POLICY["max_alerts_per_cutoff"]),
        cooldown_hours=int(POLICY["cooldown_hours"]),
    )

    observation = {
        **binary_metrics(evaluated["y_episode"], evaluated["risk_score"]),
        **threshold_metrics(
            evaluated["y_episode"],
            evaluated["risk_score"],
            float(POLICY["threshold"]),
        ),
    }
    operational = operational_metrics(evaluated)
    observed_episode_count = (
        spark.table(EPISODES_TABLE)
        .where(
            F.col("is_primary_gap")
            & F.col("episode_start_is_observed")
            & (F.col("episode_start_proxy") >= F.lit("2025-06-01").cast("timestamp_ntz"))
            & (F.col("episode_start_proxy") < F.lit("2025-07-01").cast("timestamp_ntz"))
        )
        .select("episode_id")
        .distinct()
        .count()
    )
    operational["observed_episode_count"] = float(observed_episode_count)
    operational["total_observed_episode_recall"] = (
        operational["matched_episode_count"] / observed_episode_count
        if observed_episode_count
        else 0.0
    )
    observation["lift_over_prevalence"] = (
        observation["average_precision"] / observation["prevalence"]
        if observation["prevalence"]
        else float("nan")
    )

    metric_records: list[dict[str, str | float]] = []

    def add_metrics(
        scope: str,
        values: dict[str, float],
        *,
        segment_type: str = "overall",
        segment_value: str = "all",
    ) -> None:
        for metric_name, metric_value in values.items():
            if math.isfinite(metric_value):
                metric_records.append(
                    {
                        "evaluation_id": evaluation_id,
                        "release_id": release_id,
                        "freeze_id": FREEZE_ID,
                        "scope": scope,
                        "segment_type": segment_type,
                        "segment_value": segment_value,
                        "metric_name": metric_name,
                        "metric_value": float(metric_value),
                    }
                )

    add_metrics("observation", observation)
    add_metrics("operational", operational)

    for equipment_type, segment in evaluated.groupby("tipo_equipamento"):
        add_metrics(
            "observation",
            binary_metrics(segment["y_episode"], segment["risk_score"]),
            segment_type="tipo_equipamento",
            segment_value=str(equipment_type),
        )
        add_metrics(
            "operational",
            operational_metrics(segment),
            segment_type="tipo_equipamento",
            segment_value=str(equipment_type),
        )

    for k in (1, 3, 5):
        top_k = evaluated.loc[evaluated["rank_at_cutoff"] <= k]
        selected_count = len(top_k)
        selected_positives = int(top_k["y_episode"].sum())
        all_positives = int(evaluated["y_episode"].sum())
        precision_at_k = selected_positives / selected_count if selected_count else 0.0
        recall_at_k = selected_positives / all_positives if all_positives else 0.0
        add_metrics(
            "ranking",
            {
                "k": float(k),
                "selected_row_count": float(selected_count),
                "positive_row_count": float(selected_positives),
                "precision_at_k": precision_at_k,
                "recall_at_k": recall_at_k,
                "lift_at_k": (
                    precision_at_k / observation["prevalence"]
                    if observation["prevalence"]
                    else float("nan")
                ),
            },
            segment_type="top_k",
            segment_value=str(k),
        )

    for metric_name, metric_value in observation.items():
        if math.isfinite(metric_value):
            mlflow.log_metric(f"holdout_observation_{metric_name}", metric_value)
    for metric_name, metric_value in operational.items():
        if math.isfinite(metric_value):
            mlflow.log_metric(f"holdout_operational_{metric_name}", metric_value)
    mlflow.log_input(
        mlflow.data.from_pandas(
            holdout[FEATURE_NAMES + ["y_episode"]],
            source=TRAINING_TABLE,
            name="holdout_june_released_once",
        ),
        context="holdout_evaluation",
    )

    # Juntar as decisões ao Spark original preserva TIMESTAMP_NTZ da Gold.
    prediction_payload = evaluated[
        [
            "scoring_row_id",
            "risk_score",
            "rank_at_cutoff",
            "is_policy_candidate",
            "is_emitted_alert",
            "matched_episode_id",
            "is_true_alert",
        ]
    ].copy()
    prediction_payload["matched_episode_id"] = prediction_payload[
        "matched_episode_id"
    ].where(prediction_payload["matched_episode_id"].notna(), None)
    prediction_spark = spark.createDataFrame(prediction_payload)
    scored_spark = (
        holdout_spark.select(
            "scoring_row_id",
            "prediction_timestamp",
            "tag",
            "tipo_equipamento",
            "y_episode",
            "next_episode_id",
            "next_episode_start",
            "lead_time_minutes",
        )
        .join(prediction_spark, on="scoring_row_id", how="inner")
        .withColumn("evaluation_id", F.lit(evaluation_id))
        .withColumn("release_id", F.lit(release_id))
        .withColumn("freeze_id", F.lit(FREEZE_ID))
        .withColumn("model_name", F.lit(MODEL_NAME))
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("policy_threshold", F.lit(float(POLICY["threshold"])))
        .withColumn(
            "policy_max_alerts_per_cutoff",
            F.lit(int(POLICY["max_alerts_per_cutoff"])),
        )
        .withColumn("policy_cooldown_hours", F.lit(int(POLICY["cooldown_hours"])))
        .withColumn("evaluated_at_utc", F.current_timestamp())
    )
    output_types = scored_spark.selectExpr(
        "typeof(prediction_timestamp) as prediction_type",
        "typeof(next_episode_start) as episode_type",
    ).first()
    if output_types["prediction_type"] != "timestamp_ntz":
        raise RuntimeError("prediction_timestamp perdeu o contrato TIMESTAMP_NTZ")
    if output_types["episode_type"] != "timestamp_ntz":
        raise RuntimeError("next_episode_start perdeu o contrato TIMESTAMP_NTZ")
    if scored_spark.count() != holdout_row_count:
        raise RuntimeError("join de scores alterou cardinalidade do holdout")

    merge_delta(scored_spark, HOLDOUT_SCORES_TABLE, ["evaluation_id", "scoring_row_id"])
    metrics_spark = spark.createDataFrame(pd.DataFrame(metric_records))
    merge_delta(
        metrics_spark,
        EVALUATION_METRICS_TABLE,
        ["evaluation_id", "scope", "segment_type", "segment_value", "metric_name"],
    )

    evaluation_manifest = {
        "manifest_version": "1.0.0",
        "evaluation_id": evaluation_id,
        "release_id": release_id,
        "freeze_id": FREEZE_ID,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_uri": MODEL_URI,
        "model_changed_after_freeze": False,
        "calibrator_changed_after_freeze": False,
        "policy_changed_after_freeze": False,
        "holdout_row_count": holdout_row_count,
        "observed_episode_count": observed_episode_count,
        "observation_metrics": observation,
        "operational_metrics": operational,
        "policy": POLICY,
        "source_timezone": "UNCONFIRMED",
        "previous_aggregate_label_exposure": True,
        "mlflow_run_id": evaluation_run_id,
        "job_run_id": JOB_RUN_ID,
        "task_run_id": TASK_RUN_ID,
        "sha256": {
            "freeze_manifest": freeze_manifest_sha256,
            "feature_registry": feature_registry_sha256,
            "evaluation_notebook": EVALUATION_NOTEBOOK_SHA256,
        },
    }
    mlflow.log_dict(
        evaluation_manifest, "holdout/holdout_evaluation_manifest_v1.json"
    )
    evaluation_manifest_row = spark.createDataFrame(
        [
            {
                "evaluation_id": evaluation_id,
                "release_id": release_id,
                "freeze_id": FREEZE_ID,
                "evaluated_at_utc": datetime.now(timezone.utc),
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "mlflow_run_id": evaluation_run_id,
                "job_run_id": JOB_RUN_ID,
                "task_run_id": TASK_RUN_ID,
                "manifest_json": json.dumps(evaluation_manifest, sort_keys=True),
            }
        ]
    )
    merge_delta(
        evaluation_manifest_row,
        EVALUATION_MANIFEST_TABLE,
        ["evaluation_id"],
    )

result = {
    "status": "SUCCESS",
    "evaluation_id": evaluation_id,
    "release_id": release_id,
    "freeze_id": FREEZE_ID,
    "evaluation_run_id": evaluation_run_id,
    "model_uri": MODEL_URI,
    "holdout_row_count": holdout_row_count,
    "average_precision": observation["average_precision"],
    "roc_auc": observation["roc_auc"],
    "brier": observation["brier"],
    "alert_count": operational["alert_count"],
    "episode_precision": operational["episode_precision"],
    "targetable_episode_recall": operational["targetable_episode_recall"],
    "total_observed_episode_recall": operational["total_observed_episode_recall"],
    "median_lead_time_minutes": operational["median_lead_time_minutes"],
    "policy_changed_after_freeze": False,
}
print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
