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
# # 04 — Scoring batch com pacote congelado
#
# ## Goal
#
# Produzir scores e fila de alertas para um intervalo configurável usando a
# versão imutável do modelo e a política do freeze informado. Este notebook
# consome apenas `fct_features_scoring`: labels, episódios futuros e métricas de
# desempenho não fazem parte do caminho operacional.
#
# ## Key assumptions
#
# - o modelo é carregado por versão exata, nunca por alias mutável;
# - o cooldown recebe quatro horas de contexto anteriores ao início do lote;
# - o contexto afeta a decisão, mas não é gravado como parte do lote solicitado;
# - a saída bruta vai para o schema de ML e recebe curadoria dbt em Gold;
# - importância por ganho descreve o modelo global e não explica causalidade.

# %% [markdown]
# ## 1. Setup, parâmetros e gates do freeze

# %%
from __future__ import annotations

import json
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
    "gold_schema": "",
    "ml_schema": "",
    "experiment_path": "",
    "project_files_path": "",
    "freeze_id": "",
    "score_start": "2025-06-01 00:00:00",
    "score_end": "2025-07-01 00:00:00",
    "job_run_id": "UNKNOWN",
    "task_run_id": "UNKNOWN",
}
for parameter_name, default_value in PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(parameter_name, default_value)


def parameter(name: str) -> str:
    return dbutils.widgets.get(name).strip()


CATALOG = parameter("catalog")
GOLD_SCHEMA = parameter("gold_schema")
ML_SCHEMA = parameter("ml_schema")
EXPERIMENT_PATH = parameter("experiment_path")
project_files = parameter("project_files_path")
PROJECT_FILES_PATH = Path(project_files)
FREEZE_ID = parameter("freeze_id")
SCORE_START = pd.Timestamp(parameter("score_start"))
SCORE_END = pd.Timestamp(parameter("score_end"))
JOB_RUN_ID = parameter("job_run_id")
TASK_RUN_ID = parameter("task_run_id")

if (
    pd.isna(SCORE_START) or pd.isna(SCORE_END)
    or SCORE_START.tz is not None or SCORE_END.tz is not None
    or SCORE_START >= SCORE_END
):
    raise ValueError("Informe score_start e score_end em ordem e sem fuso horário.")
if not project_files or not PROJECT_FILES_PATH.is_dir():
    raise FileNotFoundError(f"Arquivos do bundle ausentes: {PROJECT_FILES_PATH}")
if str(PROJECT_FILES_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_FILES_PATH))

from src.batch_scoring import (  # noqa: E402
    apply_operational_policy,
    extract_feature_importance,
    scoring_run_id,
    select_scoring_window,
)
from src.model_freeze import (  # noqa: E402
    load_frozen_estimator,
    read_training_freeze,
    validate_ml_namespaces,
)
from src.temporal_modeling import (  # noqa: E402
    load_feature_registry,
    sha256_file,
    to_model_matrix,
    validated_probabilities,
)

validate_ml_namespaces(CATALOG, GOLD_SCHEMA, ML_SCHEMA)
FEATURE_REGISTRY_PATH = PROJECT_FILES_PATH / "manifests/features_v1.yml"
BATCH_SCORING_CODE_PATH = PROJECT_FILES_PATH / "src/batch_scoring.py"
SCORING_NOTEBOOK_PATH = PROJECT_FILES_PATH / "notebooks/04_score_batch.py"
feature_registry, FEATURE_NAMES = load_feature_registry(FEATURE_REGISTRY_PATH)
feature_registry_sha256 = sha256_file(FEATURE_REGISTRY_PATH)
batch_scoring_code_sha256 = sha256_file(BATCH_SCORING_CODE_PATH)
scoring_notebook_sha256 = sha256_file(SCORING_NOTEBOOK_PATH)
MODEL_NAME = f"{CATALOG}.{ML_SCHEMA}.dont_go_episode_risk"

FEATURES_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_features_scoring"
FREEZE_TABLE = f"{CATALOG}.{ML_SCHEMA}.model_freeze_manifest"
SCORES_TABLE = f"{CATALOG}.{ML_SCHEMA}.batch_scoring_scores"
RUNS_TABLE = f"{CATALOG}.{ML_SCHEMA}.batch_scoring_runs"
IMPORTANCE_TABLE = f"{CATALOG}.{ML_SCHEMA}.model_feature_importance"

freeze_manifest, freeze_manifest_sha256, _ = read_training_freeze(
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
CONTEXT_START = SCORE_START - pd.Timedelta(hours=int(POLICY["cooldown_hours"]))

SCORING_RUN_ID = scoring_run_id(
    freeze_id=FREEZE_ID,
    model_name=MODEL_NAME,
    model_version=MODEL_VERSION,
    feature_registry_sha256=feature_registry_sha256,
    policy=POLICY,
    score_start=SCORE_START.isoformat(),
    score_end=SCORE_END.isoformat(),
)

print(f"Scoring run ID         : {SCORING_RUN_ID}")
print(f"Modelo imutável        : {MODEL_URI}")
print(f"Intervalo solicitado   : [{SCORE_START}, {SCORE_END})")
print(f"Contexto do cooldown   : [{CONTEXT_START}, {SCORE_START})")

# %% [markdown]
# ## 2. Carregar modelo e validar a proveniência do artefato

# %%
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.autolog(disable=True)
frozen_model = load_frozen_estimator(freeze_manifest)

# %% [markdown]
# ## 3. Scoring target-free com contexto de cooldown

# %%
prediction_timestamp = F.col("prediction_timestamp")
context_start_ntz = F.lit(CONTEXT_START.isoformat(sep=" ")).cast("timestamp_ntz")
score_start_ntz = F.lit(SCORE_START.isoformat(sep=" ")).cast("timestamp_ntz")
score_end_ntz = F.lit(SCORE_END.isoformat(sep=" ")).cast("timestamp_ntz")

source_with_context = (
    spark.table(FEATURES_TABLE)
    .where(
        (prediction_timestamp >= context_start_ntz)
        & (prediction_timestamp < score_end_ntz)
    )
    .select(
        "scoring_row_id",
        "prediction_timestamp",
        "tag",
        "tipo_equipamento",
        "feature_set_version",
        "max_feature_event_time_24h",
        "max_feature_availability_time_24h",
        *FEATURE_NAMES,
    )
)
source_row_count_with_context = source_with_context.count()
if source_row_count_with_context == 0:
    raise RuntimeError("intervalo solicitado não contém linhas elegíveis")
temporal_violations = source_with_context.where(
    (F.col("max_feature_event_time_24h") >= F.col("prediction_timestamp"))
    | (F.col("max_feature_availability_time_24h") > F.col("prediction_timestamp"))
).count()
if temporal_violations:
    raise RuntimeError(f"violações point-in-time no lote: {temporal_violations}")

source_pd = source_with_context.toPandas()
if source_pd["scoring_row_id"].duplicated().any():
    raise RuntimeError("lote contém scoring_row_id duplicado")
source_pd["risk_score"] = validated_probabilities(
    frozen_model.predict_proba(to_model_matrix(source_pd, FEATURE_NAMES))[:, 1]
)
scored_with_context = apply_operational_policy(
    source_pd,
    threshold=float(POLICY["threshold"]),
    max_alerts_per_cutoff=int(POLICY["max_alerts_per_cutoff"]),
    cooldown_hours=int(POLICY["cooldown_hours"]),
)
scored = select_scoring_window(
    scored_with_context,
    score_start=SCORE_START.isoformat(),
    score_end=SCORE_END.isoformat(),
)
if scored.empty:
    raise RuntimeError("janela de saída não contém linhas elegíveis")

prediction_payload = scored[
    [
        "scoring_row_id",
        "risk_score",
        "rank_at_cutoff",
        "is_policy_candidate",
        "is_emitted_alert",
    ]
].copy()
prediction_spark = spark.createDataFrame(prediction_payload)
business_window = source_with_context.where(
    (prediction_timestamp >= score_start_ntz) & (prediction_timestamp < score_end_ntz)
).select("scoring_row_id", "prediction_timestamp", "tag", "tipo_equipamento")
scored_spark = (
    business_window.join(prediction_spark, on="scoring_row_id", how="inner")
    .withColumn("scoring_run_id", F.lit(SCORING_RUN_ID))
    .withColumn("freeze_id", F.lit(FREEZE_ID))
    .withColumn("model_name", F.lit(MODEL_NAME))
    .withColumn("model_version", F.lit(MODEL_VERSION))
    .withColumn("feature_set_version", F.lit(feature_registry["version"]))
    .withColumn("score_start", F.lit(SCORE_START.to_pydatetime()).cast("timestamp_ntz"))
    .withColumn("score_end", F.lit(SCORE_END.to_pydatetime()).cast("timestamp_ntz"))
    .withColumn("policy_threshold", F.lit(float(POLICY["threshold"])))
    .withColumn(
        "policy_max_alerts_per_cutoff",
        F.lit(int(POLICY["max_alerts_per_cutoff"])),
    )
    .withColumn("policy_cooldown_hours", F.lit(int(POLICY["cooldown_hours"])))
    .withColumn("scored_at_utc", F.current_timestamp())
)
output_row_count = scored_spark.count()
if output_row_count != len(scored):
    raise RuntimeError("join de scores alterou a cardinalidade do lote")
if (
    scored_spark.selectExpr("typeof(prediction_timestamp) AS t").first()["t"]
    != "timestamp_ntz"
):
    raise RuntimeError("prediction_timestamp perdeu o contrato TIMESTAMP_NTZ")

# %% [markdown]
# ## 4. Persistência idempotente, importância global e rastreabilidade


# %%
def insert_only_delta(source: DataFrame, table_name: str, keys: list[str]) -> None:
    """Insere chaves novas sem reescrever uma execução já registrada."""
    if not spark.catalog.tableExists(table_name):
        source.write.saveAsTable(table_name)
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(source.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


importance = extract_feature_importance(frozen_model, FEATURE_NAMES)
importance["freeze_id"] = FREEZE_ID
importance["model_name"] = MODEL_NAME
importance["model_version"] = MODEL_VERSION
importance["feature_set_version"] = feature_registry["version"]
importance_spark = spark.createDataFrame(importance).withColumn(
    "extracted_at_utc", F.current_timestamp()
)

with mlflow.start_run(
    run_name=f"batch_scoring_{SCORE_START:%Y%m%d}_{SCORE_END:%Y%m%d}"
) as run:
    mlflow_run_id = run.info.run_id
    mlflow.set_tags(
        {
            "stage": "batch_scoring",
            "freeze_id": FREEZE_ID,
            "model_version": MODEL_VERSION,
            "label_access": "none",
        }
    )
    mlflow.log_params(
        {
            "scoring_run_id": SCORING_RUN_ID,
            "model_uri": MODEL_URI,
            "feature_count": len(FEATURE_NAMES),
            "score_start": SCORE_START.isoformat(),
            "score_end": SCORE_END.isoformat(),
            "context_start": CONTEXT_START.isoformat(),
            "policy_threshold": float(POLICY["threshold"]),
            "policy_max_alerts_per_cutoff": int(POLICY["max_alerts_per_cutoff"]),
            "policy_cooldown_hours": int(POLICY["cooldown_hours"]),
        }
    )
    mlflow.log_metrics(
        {
            "scoring_row_count": float(output_row_count),
            "candidate_count": float(scored["is_policy_candidate"].sum()),
            "emitted_alert_count": float(scored["is_emitted_alert"].sum()),
            "mean_risk_score": float(scored["risk_score"].mean()),
        }
    )
    mlflow.log_input(
        mlflow.data.from_pandas(
            source_pd[FEATURE_NAMES],
            source=FEATURES_TABLE,
            name="target_free_feature_batch",
        ),
        context="batch_scoring",
    )
    mlflow.log_table(importance, artifact_file="model/feature_importance_gain.json")

    run_manifest = {
        "manifest_version": "1.0.0",
        "scoring_run_id": SCORING_RUN_ID,
        "freeze_id": FREEZE_ID,
        "model_uri": MODEL_URI,
        "feature_set_version": feature_registry["version"],
        "score_start": SCORE_START.isoformat(),
        "score_end": SCORE_END.isoformat(),
        "context_start": CONTEXT_START.isoformat(),
        "source_row_count_with_context": source_row_count_with_context,
        "output_row_count": output_row_count,
        "candidate_count": int(scored["is_policy_candidate"].sum()),
        "emitted_alert_count": int(scored["is_emitted_alert"].sum()),
        "label_access": "NONE",
        "policy": POLICY,
        "mlflow_run_id": mlflow_run_id,
        "job_run_id": JOB_RUN_ID,
        "task_run_id": TASK_RUN_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": {
            "freeze_manifest": freeze_manifest_sha256,
            "feature_registry": feature_registry_sha256,
            "batch_scoring_code": batch_scoring_code_sha256,
            "scoring_notebook": scoring_notebook_sha256,
            "model_artifact_tree": freeze_manifest["sha256"]["model_artifact_tree"],
        },
    }
    mlflow.log_dict(run_manifest, "scoring/batch_scoring_manifest_v1.json")

    run_spark = spark.createDataFrame(
        [
            {
                "scoring_run_id": SCORING_RUN_ID,
                "freeze_id": FREEZE_ID,
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "feature_set_version": feature_registry["version"],
                "score_start": SCORE_START.to_pydatetime(),
                "score_end": SCORE_END.to_pydatetime(),
                "context_start": CONTEXT_START.to_pydatetime(),
                "source_row_count_with_context": source_row_count_with_context,
                "output_row_count": output_row_count,
                "candidate_count": int(scored["is_policy_candidate"].sum()),
                "emitted_alert_count": int(scored["is_emitted_alert"].sum()),
                "label_access": "NONE",
                "mlflow_run_id": mlflow_run_id,
                "job_run_id": JOB_RUN_ID,
                "task_run_id": TASK_RUN_ID,
                "created_at_utc": datetime.now(timezone.utc),
                "manifest_json": json.dumps(run_manifest, sort_keys=True),
            }
        ]
    )
    insert_only_delta(scored_spark, SCORES_TABLE, ["scoring_run_id", "scoring_row_id"])
    insert_only_delta(run_spark, RUNS_TABLE, ["scoring_run_id"])
    insert_only_delta(importance_spark, IMPORTANCE_TABLE, ["freeze_id", "feature_name"])

persisted_scores = spark.table(SCORES_TABLE).where(
    F.col("scoring_run_id") == SCORING_RUN_ID
)
if persisted_scores.count() != output_row_count:
    raise RuntimeError("persistência do lote não reconciliou a cardinalidade")

result = {
    "status": "SUCCESS",
    "scoring_run_id": SCORING_RUN_ID,
    "freeze_id": FREEZE_ID,
    "model_uri": MODEL_URI,
    "score_start": SCORE_START.isoformat(),
    "score_end": SCORE_END.isoformat(),
    "output_row_count": output_row_count,
    "candidate_count": int(scored["is_policy_candidate"].sum()),
    "emitted_alert_count": int(scored["is_emitted_alert"].sum()),
    "mlflow_run_id": mlflow_run_id,
    "label_access": "NONE",
}
print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
