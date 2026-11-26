"""Load and verify a model frozen by the training job in this workspace."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from .temporal_modeling import sha256_tree


def validate_ml_namespaces(catalog: str, *schemas: str) -> None:
    """Reject invalid identifiers or ambiguous destinations before connecting."""
    if not all(re.fullmatch(r"[a-z][a-z0-9_]*", part) for part in (catalog, *schemas)):
        raise ValueError("Configure catálogo e schemas com identificadores válidos.")
    if len(set(schemas)) != len(schemas):
        raise ValueError("Use um schema diferente para cada camada.")


def training_freeze_id(manifest: dict[str, Any]) -> str:
    """Address the complete freeze by its content, excluding its own identifier."""
    payload = {key: value for key, value in manifest.items() if key != "freeze_id"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def parse_training_freeze(
    row: dict[str, Any],
    freeze_id: str,
    model_name: str,
    feature_set_version: str,
    feature_names: list[str],
    feature_registry_sha256: str,
) -> tuple[dict[str, Any], str]:
    """Check the persisted training manifest against its columns and consumers."""
    manifest_json = row["manifest_json"]
    manifest = json.loads(manifest_json)
    policy = manifest["policy"]
    hashes = manifest["sha256"]
    expected = {
        "freeze_id": freeze_id,
        "model_name": model_name,
        "model_version": manifest["model_version"],
        "final_run_id": manifest["final_run_id"],
        "feature_set_version": feature_set_version,
        "feature_registry_sha256": feature_registry_sha256,
        "modeling_code_sha256": hashes["modeling_code"],
        "training_notebook_sha256": hashes["training_notebook"],
        "model_artifact_sha256": hashes["model_artifact_tree"],
        "policy_threshold": policy["threshold"],
        "policy_max_alerts_per_cutoff": policy["max_alerts_per_cutoff"],
        "policy_cooldown_hours": policy["cooldown_hours"],
        "holdout_released": False,
    }
    if any(row[key] != value for key, value in expected.items()):
        raise ValueError("A linha de freeze diverge do manifesto ou da configuração.")
    if (
        manifest["manifest_version"] != "1.0.0"
        or manifest["freeze_id"] != freeze_id
        or training_freeze_id(manifest) != freeze_id
        or manifest["model_name"] != model_name
        or not re.fullmatch(r"[1-9][0-9]*", manifest["model_version"])
        or manifest["feature_set_version"] != feature_set_version
        or manifest["feature_names"] != feature_names
        or manifest["feature_count"] != len(feature_names)
        or hashes["feature_registry"] != feature_registry_sha256
        or manifest["holdout_released"] is not False
    ):
        raise ValueError("O manifesto de treino diverge da versão ou das features esperadas.")

    digest = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
    return manifest, digest


def read_training_freeze(
    spark: Any,
    table: str,
    freeze_id: str,
    model_name: str,
    feature_set_version: str,
    feature_names: list[str],
    feature_registry_sha256: str,
) -> tuple[dict[str, Any], str, str]:
    """Read one explicitly selected freeze, preserving the original manifest bytes."""
    if not re.fullmatch(r"[0-9a-f]{64}", freeze_id):
        raise ValueError("Informe o freeze_id retornado pelo Job de treino.")

    rows = spark.table(table).where(f"freeze_id = '{freeze_id}'").limit(2).collect()
    if len(rows) != 1:
        raise ValueError("O freeze informado precisa existir uma única vez.")

    row = rows[0].asDict()
    manifest, digest = parse_training_freeze(
        row,
        freeze_id,
        model_name,
        feature_set_version,
        feature_names,
        feature_registry_sha256,
    )
    return manifest, digest, row["manifest_json"]


def load_frozen_estimator(manifest: dict[str, Any]) -> Any:
    """Verify the exact registered version and deserialize the checked bytes."""
    import mlflow
    import mlflow.sklearn
    from mlflow import MlflowClient

    name = manifest["model_name"]
    version = manifest["model_version"]
    client = MlflowClient(registry_uri="databricks-uc")
    if client.get_model_version(name, version).run_id != manifest["final_run_id"]:
        raise RuntimeError("O run de origem da versão registrada diverge do freeze.")

    with TemporaryDirectory(prefix="frota-model-") as temporary_dir:
        model_path = Path(
            mlflow.artifacts.download_artifacts(
                artifact_uri=f"models:/{name}/{version}",
                dst_path=temporary_dir,
            )
        )
        if sha256_tree(model_path) != manifest["sha256"]["model_artifact_tree"]:
            raise RuntimeError("A árvore do pacote registrado diverge do freeze.")
        return mlflow.sklearn.load_model(str(model_path))
