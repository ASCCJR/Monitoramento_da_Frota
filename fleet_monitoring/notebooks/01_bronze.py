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
# # 01 — Bronze imutável e manifesto
#
# ## Objetivo
#
# Preservar os nove inputs públicos do manifesto `1.0.0` em um Volume Unity
# Catalog, materializar quatro tabelas Delta Bronze e provar a reconciliação de
# arquivos, hashes, schemas e contagens.
#
# ## Premissas
#
# - quem executa escolhe o perfil local e o catálogo de escrita autorizado;
# - os quatro schemas são exclusivos da instalação deste projeto;
# - os timestamps de negócio permanecem `TIMESTAMP_NTZ`, pois o fuso da fonte
#   ainda está `UNCONFIRMED`;
# - o parquet de apontamentos original é preservado bit a bit; uma cópia técnica
#   em microssegundos é criada porque o Serverless rejeita `TIMESTAMP(NANOS)`;
# - a aba `CMA` e as duas abas do dicionário são materializadas; as demais abas
#   do arquivo de alarmes continuam preservadas no XLSX original do Volume.

# %% [markdown]
# ## 1. Setup e destinos protegidos

# %%
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def find_repo_root() -> Path:
    """Encontra a raiz tanto em execução local quanto em um Git folder."""
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.extend(Path(__file__).resolve().parents)
    candidates.extend([Path.cwd(), *Path.cwd().parents])
    for candidate in candidates:
        if (candidate / "fleet_monitoring/manifests/inputs_v1.json").is_file():
            return candidate
    raise FileNotFoundError("Não foi possível localizar a raiz do repositório")


ROOT = find_repo_root()
RUNNING_IN_DBR = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fleet_monitoring.src.bronze_ingestion import (  # noqa: E402
    ensure_spark_timestamp_precision,
    git_snapshot,
    load_manifest,
    manifest_volume_path,
    prepared_volume_path,
    source_volume_path,
    upload_immutable_file,
    validate_identifier,
    write_json,
)
from fleet_monitoring.src.input_manifest import (  # noqa: E402
    sha256_file,
    validate_manifest,
)

PROFILE = None if RUNNING_IN_DBR else os.environ["DATABRICKS_CONFIG_PROFILE"]
CATALOG = os.environ["FLEET_CATALOG"]
BRONZE_SCHEMA = os.environ.get("FLEET_BRONZE_SCHEMA", "fleet_bronze")
SILVER_SCHEMA = os.environ.get("FLEET_SILVER_SCHEMA", "fleet_silver")
GOLD_SCHEMA = os.environ.get("FLEET_GOLD_SCHEMA", "fleet_gold")
ML_SCHEMA = os.environ.get("FLEET_ML_SCHEMA", "fleet_ml")
VOLUME = os.environ.get("FLEET_INPUT_VOLUME", "raw_inputs")
VOLUME_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}"
MANIFEST_PATH = ROOT / "fleet_monitoring/manifests/inputs_v1.json"

required_schemas = {BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, ML_SCHEMA}
if len(required_schemas) != 4:
    raise ValueError("Cada camada precisa de um schema distinto")
for name in required_schemas | {CATALOG, VOLUME}:
    validate_identifier(name)
manifest = load_manifest(MANIFEST_PATH)
manifest_version = manifest["manifest_version"]
source_timezone = manifest["source_timezone"]
ingestion_started_at = datetime.now(timezone.utc)

print(f"Repo root              : {ROOT}")
print(f"Modo                    : {'DBR' if RUNNING_IN_DBR else 'Spark Connect local'}")
print(f"Destino                 : {CATALOG}.{BRONZE_SCHEMA}")
print(f"Volume                  : {VOLUME_ROOT}")
print(f"Manifesto               : {manifest_version}")
print(f"Fuso da fonte           : {source_timezone}")

# %% [markdown]
# ## 2. Validar os inputs locais antes de qualquer upload
#
# A execução local exige 36 `PASS`: existência, bytes, registros e SHA-256 para
# cada um dos nove arquivos. Em DBR, os arquivos originais já devem estar no
# Volume e a reconciliação remota abaixo continua obrigatória.

# %%
if RUNNING_IN_DBR:
    print("Validação local: SKIP controlado — execução dentro do DBR")
else:
    local_checks = validate_manifest(MANIFEST_PATH, ROOT)
    failed_local_checks = [check for check in local_checks if check["status"] != "PASS"]
    if failed_local_checks:
        raise RuntimeError(f"Manifesto local divergente: {failed_local_checks}")
    print(f"INPUT MANIFEST LOCAL: PASS ({len(local_checks)} checks)")

# %% [markdown]
# ## 3. Conectar ao workspace e preparar os schemas e o Volume

# %%
if RUNNING_IN_DBR:
    spark = DatabricksSession.builder.getOrCreate()
    workspace_client = WorkspaceClient()
else:
    spark = (
        DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
    )
    workspace_client = WorkspaceClient(profile=PROFILE)

identity = spark.sql(
    "SELECT current_user() AS current_user, current_catalog() AS current_catalog"
).collect()[0]
for schema in sorted(required_schemas):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}").collect()
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME}").collect()

volume_info = workspace_client.volumes.read(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.{VOLUME}"
)
if str(volume_info.volume_type) not in {"VolumeType.MANAGED", "MANAGED"}:
    raise RuntimeError(f"Volume inesperado: {volume_info.volume_type}")

print(f"Usuário conectado       : {identity['current_user']}")
print(f"Catálogo corrente       : {identity['current_catalog']}")
print(f"Schemas                 : {sorted(required_schemas)}")
print(f"Volume gerenciado       : {volume_info.full_name}")

# %% [markdown]
# ## 4. Preservar originais no Volume e validar o conteúdo remoto
#
# Cada caminho contém versão do manifesto, papel e SHA-256. Uploads nunca usam
# overwrite. Se o arquivo já existir, ele só é reutilizado depois que bytes e
# hash remoto forem recalculados e aprovados.

# %%
source_uploads: list[dict[str, Any]] = []
inputs_with_volume_paths: list[dict[str, Any]] = []

for input_item in manifest["inputs"]:
    item = dict(input_item)
    remote_path = source_volume_path(VOLUME_ROOT, manifest_version, item)
    item["volume_path"] = remote_path

    if RUNNING_IN_DBR:
        local_path = Path(remote_path)
    else:
        local_path = ROOT / item["path"]

    upload = upload_immutable_file(
        workspace_client.files,
        local_path,
        remote_path,
        expected_sha256=item["sha256"],
        expected_bytes=item["bytes"],
    )
    upload["role"] = item["role"]
    upload["source_path"] = item["path"]
    source_uploads.append(upload)
    inputs_with_volume_paths.append(item)
    print(
        f"[{upload['status']}] {item['path']} | "
        f"bytes={upload['bytes']} | sha256={upload['sha256'][:12]}..."
    )

manifest_remote_path = manifest_volume_path(VOLUME_ROOT, manifest_version)
manifest_upload = upload_immutable_file(
    workspace_client.files,
    MANIFEST_PATH,
    manifest_remote_path,
    expected_sha256=sha256_file(MANIFEST_PATH),
    expected_bytes=MANIFEST_PATH.stat().st_size,
)
print(
    f"[{manifest_upload['status']}] manifesto | "
    f"sha256={manifest_upload['sha256'][:12]}..."
)

# %% [markdown]
# ## 5. Criar apenas o derivado técnico necessário para o Spark
#
# `Inicio` e `Fim` são `timestamp[ns]` no parquet original. O cast para
# microssegundos proíbe truncamento; se qualquer valor tiver nanossegundos
# significativos, a execução falha. O arquivo original permanece intocado.

# %%
item_apontamentos = next(
    item for item in inputs_with_volume_paths if item["role"] == "raw_apontamentos"
)
apontamentos_local_or_volume = (
    Path(item_apontamentos["volume_path"])
    if RUNNING_IN_DBR
    else ROOT / item_apontamentos["path"]
)
with TemporaryDirectory(prefix="fleet-bronze-") as temporary_dir:
    prepared_apontamentos = ensure_spark_timestamp_precision(
        apontamentos_local_or_volume, Path(temporary_dir)
    )
    prepared_apontamentos_sha256 = sha256_file(prepared_apontamentos)
    prepared_apontamentos_remote = prepared_volume_path(
        VOLUME_ROOT,
        manifest_version,
        item_apontamentos["role"],
        prepared_apontamentos_sha256,
        prepared_apontamentos.name,
    )
    prepared_upload = upload_immutable_file(
        workspace_client.files,
        prepared_apontamentos,
        prepared_apontamentos_remote,
        expected_sha256=prepared_apontamentos_sha256,
        expected_bytes=prepared_apontamentos.stat().st_size,
    )
print(
    f"[{prepared_upload['status']}] derivado timestamp_us | "
    f"bytes={prepared_upload['bytes']} | sha256={prepared_upload['sha256']}"
)

# %% [markdown]
# ## 6. Materializar as quatro tabelas Delta Bronze
#
# Nenhum valor de negócio é limpo ou recortado. `TAG` continua `TAG` na
# telemetria. O dicionário recebe nomes técnicos em snake_case apenas porque os
# cabeçalhos originais contêm espaços; os cabeçalhos e todas as abas continuam
# preservados no XLSX imutável.

# %%
PROVENANCE_COLUMNS = [
    "_source_path",
    "_source_sha256",
    "_source_period",
    "_manifest_version",
    "_ingested_at_utc",
]

EXPECTED_BASE_COLUMNS = {
    "apontamentos_raw": ["Id", "Inicio", "Fim", "Tag", "Frota", "Tipo", "Classe"],
    "telemetria_raw": [
        "Id_Eventos_Telemetria",
        "Data_Evento",
        "Inicio_Turno",
        "Fim_Turno",
        "Dia",
        "Localidade",
        "TAG",
        "Tag_Frota",
        "Tipo",
        "Nome_Operador_Anon",
        "Matricula_Operador_Hash",
        "Id_Alarme",
        "Alarme",
        "Id_Criticidade",
        "Criticidade",
        "Valor",
        "Classe",
        "Is_Dont_Go",
    ],
    "cma_regras_raw": ["TIPO", "EVENTO", "SITUACAO", "QUANTIDADE", "TEMPO", "NIVEL"],
    "dicionario_dados_raw": [
        "nome_tabela",
        "nome_feature",
        "tipo_dados",
        "descricao",
        "_source_sheet",
    ],
}


def add_provenance(frame: DataFrame, item: dict[str, Any]) -> DataFrame:
    """Adiciona somente metadados técnicos previstos no contrato Bronze."""
    return (
        frame.withColumn("_source_path", F.lit(item["path"]))
        .withColumn("_source_sha256", F.lit(item["sha256"]))
        .withColumn("_source_period", F.lit(item.get("period")).cast("string"))
        .withColumn("_manifest_version", F.lit(manifest_version))
        .withColumn("_ingested_at_utc", F.lit(ingestion_started_at))
    )


def validate_base_columns(frame: DataFrame, dataset_name: str) -> None:
    actual = [column for column in frame.columns if column not in PROVENANCE_COLUMNS]
    expected = EXPECTED_BASE_COLUMNS[dataset_name]
    if actual != expected:
        raise RuntimeError(
            f"Schema divergente em {dataset_name}: esperado={expected}, atual={actual}"
        )


def table_exists(table_name: str) -> bool:
    return bool(spark.catalog.tableExists(table_name))


def write_immutable_table(
    frame: DataFrame,
    dataset_name: str,
    comment: str,
    partition_columns: list[str] | None = None,
) -> str:
    """Cria uma tabela uma vez; reexecuções só podem validar a mesma versão."""
    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}"
    validate_base_columns(frame, dataset_name)

    if table_exists(table_name):
        details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
        properties = details.get("properties") or {}
        if properties.get("portfolio_v2.manifest_version") != manifest_version:
            raise RuntimeError(
                f"{table_name} já existe com outro manifesto: {properties}"
            )
        return "REUSED"

    properties = {
        "comment": comment,
        "portfolio_v2.manifest_version": manifest_version,
        "portfolio_v2.contract_version": manifest["contract_version"],
        "portfolio_v2.source_timezone": source_timezone,
        "portfolio_v2.immutable_source": "true",
    }
    writer = frame.writeTo(table_name).using("delta")
    for name, value in properties.items():
        writer.tableProperty(name, value)
    if partition_columns:
        writer.partitionedBy(*partition_columns)
    writer.create()
    return "CREATED"


# Apontamentos: usa a cópia técnica em microssegundos, com proveniência do original.
df_apontamentos = add_provenance(
    spark.read.parquet(prepared_apontamentos_remote),
    item_apontamentos,
)

# Telemetria: lê cada arquivo explicitamente para preservar a proveniência mensal.
telemetry_items = [
    item for item in inputs_with_volume_paths if item["role"] == "raw_telemetria"
]
telemetry_frames = [
    add_provenance(spark.read.parquet(item["volume_path"]), item)
    for item in telemetry_items
]
df_telemetria = reduce(
    lambda left, right: left.unionByName(right),
    telemetry_frames,
)

# CMA: a única aba autorizada para alimentar as regras na v2.
item_cma = next(
    item for item in inputs_with_volume_paths if item["role"] == "cma_regras_revisado"
)
cma_local_or_volume = (
    Path(item_cma["volume_path"]) if RUNNING_IN_DBR else ROOT / item_cma["path"]
)
cma_pandas = pd.read_excel(cma_local_or_volume, sheet_name=item_cma["sheet"])
cma_records = [
    (
        str(row.TIPO),
        str(row.EVENTO),
        str(row.SITUACAO),
        int(row.QUANTIDADE),
        int(row.TEMPO),
        str(row.NIVEL),
    )
    for row in cma_pandas.itertuples(index=False)
]
cma_schema = T.StructType(
    [
        T.StructField("TIPO", T.StringType(), False),
        T.StructField("EVENTO", T.StringType(), False),
        T.StructField("SITUACAO", T.StringType(), False),
        T.StructField("QUANTIDADE", T.LongType(), False),
        T.StructField("TEMPO", T.LongType(), False),
        T.StructField("NIVEL", T.StringType(), False),
    ]
)
df_cma = add_provenance(spark.createDataFrame(cma_records, cma_schema), item_cma)

# Dicionário: união explícita das duas abas, mantendo a aba como proveniência.
item_dictionary = next(
    item for item in inputs_with_volume_paths if item["role"] == "dicionario_revisado"
)
dictionary_local_or_volume = (
    Path(item_dictionary["volume_path"])
    if RUNNING_IN_DBR
    else ROOT / item_dictionary["path"]
)
dictionary_records: list[tuple[str, str, str, str, str]] = []
for sheet_name in item_dictionary["sheets"]:
    sheet = pd.read_excel(dictionary_local_or_volume, sheet_name=sheet_name)
    dictionary_records.extend(
        (
            str(row["Nome da Tabela"]),
            str(row["Nome da Feature"]),
            str(row["Tipo de Dados"]),
            str(row["Descrição"]),
            sheet_name,
        )
        for row in sheet.to_dict(orient="records")
    )
dictionary_schema = T.StructType(
    [
        T.StructField("nome_tabela", T.StringType(), False),
        T.StructField("nome_feature", T.StringType(), False),
        T.StructField("tipo_dados", T.StringType(), False),
        T.StructField("descricao", T.StringType(), False),
        T.StructField("_source_sheet", T.StringType(), False),
    ]
)
df_dictionary = add_provenance(
    spark.createDataFrame(dictionary_records, dictionary_schema),
    item_dictionary,
)

table_actions = {
    "apontamentos_raw": write_immutable_table(
        df_apontamentos,
        "apontamentos_raw",
        "Apontamentos brutos da v2 com proveniência do manifesto 1.0.0.",
    ),
    "telemetria_raw": write_immutable_table(
        df_telemetria,
        "telemetria_raw",
        "Telemetria bruta consolidada da v2 com proveniência mensal.",
        partition_columns=["_source_period"],
    ),
    "cma_regras_raw": write_immutable_table(
        df_cma,
        "cma_regras_raw",
        "Aba CMA revisada, sem execução das regras QUANTIDADE por TEMPO.",
    ),
    "dicionario_dados_raw": write_immutable_table(
        df_dictionary,
        "dicionario_dados_raw",
        "Dicionário revisado das abas Apontamentos e Telemetria.",
    ),
}
print(f"Ações nas tabelas: {table_actions}")

# %% [markdown]
# ## 7. Reconciliar tabelas, fontes e propriedades

# %%
expected_rows_by_table = {
    "apontamentos_raw": manifest["expected_totals"]["raw_apontamentos_records"],
    "telemetria_raw": manifest["expected_totals"]["raw_telemetria_records"],
    "cma_regras_raw": manifest["expected_totals"]["cma_rules"],
    "dicionario_dados_raw": manifest["expected_totals"]["dictionary_fields"],
}
expected_sources_by_table = {
    "apontamentos_raw": [item_apontamentos],
    "telemetria_raw": telemetry_items,
    "cma_regras_raw": [item_cma],
    "dicionario_dados_raw": [item_dictionary],
}


def validate_materialized_table(dataset_name: str) -> dict[str, Any]:
    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}"
    frame = spark.table(table_name)
    validate_base_columns(frame, dataset_name)

    actual_total = frame.count()
    expected_total = expected_rows_by_table[dataset_name]
    if actual_total != expected_total:
        raise RuntimeError(
            f"Contagem divergente em {table_name}: "
            f"esperado={expected_total}, atual={actual_total}"
        )

    actual_source_counts = {
        row["_source_path"]: row["count"]
        for row in frame.groupBy("_source_path").count().collect()
    }
    expected_source_counts = {
        item["path"]: item["records"] for item in expected_sources_by_table[dataset_name]
    }
    if actual_source_counts != expected_source_counts:
        raise RuntimeError(
            f"Reconciliação por fonte falhou em {table_name}: "
            f"esperado={expected_source_counts}, atual={actual_source_counts}"
        )

    actual_hashes = {
        row["_source_sha256"]
        for row in frame.select("_source_sha256").distinct().collect()
    }
    expected_hashes = {
        item["sha256"] for item in expected_sources_by_table[dataset_name]
    }
    if actual_hashes != expected_hashes:
        raise RuntimeError(
            f"Hashes de proveniência divergentes em {table_name}: {actual_hashes}"
        )

    null_metadata = frame.filter(
        F.col("_source_path").isNull()
        | F.col("_source_sha256").isNull()
        | F.col("_manifest_version").isNull()
        | F.col("_ingested_at_utc").isNull()
    ).count()
    if null_metadata:
        raise RuntimeError(f"{table_name} tem {null_metadata} metadados nulos")

    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
    properties = detail.get("properties") or {}
    if properties.get("portfolio_v2.manifest_version") != manifest_version:
        raise RuntimeError(f"Propriedade do manifesto divergente em {table_name}")

    ingested_range = frame.agg(
        F.min("_ingested_at_utc").alias("min_ingested_at_utc"),
        F.max("_ingested_at_utc").alias("max_ingested_at_utc"),
    ).collect()[0]
    return {
        "table": table_name,
        "action": table_actions[dataset_name],
        "rows": actual_total,
        "columns": frame.columns,
        "source_counts": actual_source_counts,
        "source_hashes": sorted(actual_hashes),
        "delta_table_id": detail.get("id"),
        "delta_format": detail.get("format"),
        "num_files": detail.get("numFiles"),
        "size_in_bytes": detail.get("sizeInBytes"),
        "min_ingested_at_utc": ingested_range["min_ingested_at_utc"].isoformat(),
        "max_ingested_at_utc": ingested_range["max_ingested_at_utc"].isoformat(),
        "status": "PASS",
    }


table_results = {
    dataset_name: validate_materialized_table(dataset_name)
    for dataset_name in expected_rows_by_table
}

for dataset_name, result in table_results.items():
    print(
        f"[PASS] {dataset_name}: rows={result['rows']:,}, "
        f"sources={len(result['source_hashes'])}, files={result['num_files']}"
    )

# %% [markdown]
# ## 8. Registrar a execução com código e estado Git

# %%
script_path = ROOT / "fleet_monitoring/notebooks/01_bronze.py"
notebook_path = ROOT / "fleet_monitoring/notebooks/01_bronze.ipynb"
execution_manifest_path = (
    ROOT / "outputs/bronze/bronze_execution_manifest_v1.json"
)

execution_manifest = {
    "artifact": "portfolio_v2_bronze_execution",
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "execution_mode": "DBR" if RUNNING_IN_DBR else "databricks-connect-serverless",
    "profile": None if RUNNING_IN_DBR else PROFILE,
    "workspace_user": identity["current_user"],
    "catalog": CATALOG,
    "schema": BRONZE_SCHEMA,
    "volume": VOLUME_ROOT,
    "manifest_version": manifest_version,
    "manifest_sha256": sha256_file(MANIFEST_PATH),
    "contract_version": manifest["contract_version"],
    "source_timezone": source_timezone,
    "script_path": str(script_path.relative_to(ROOT)),
    "script_sha256": sha256_file(script_path),
    "notebook_path": str(notebook_path.relative_to(ROOT)),
    "notebook_sha256": sha256_file(notebook_path) if notebook_path.is_file() else None,
    "git": git_snapshot(ROOT),
    "source_uploads": source_uploads,
    "manifest_upload": manifest_upload,
    "prepared_apontamentos": {
        **prepared_upload,
        "derived_from_sha256": item_apontamentos["sha256"],
        "reason": "Spark Serverless rejects TIMESTAMP(NANOS)",
    },
    "tables": table_results,
    "status": "PASS",
}
write_json(execution_manifest_path, execution_manifest)

execution_manifest_sha256 = sha256_file(execution_manifest_path)
execution_manifest_remote = prepared_volume_path(
    VOLUME_ROOT,
    manifest_version,
    "execution_manifest",
    execution_manifest_sha256,
    execution_manifest_path.name,
)
execution_upload = upload_immutable_file(
    workspace_client.files,
    execution_manifest_path,
    execution_manifest_remote,
    expected_sha256=execution_manifest_sha256,
    expected_bytes=execution_manifest_path.stat().st_size,
)

print(f"Execution manifest local : {execution_manifest_path}")
print(f"Execution manifest remoto: {execution_upload['remote_path']}")
print(f"Execution manifest SHA-256: {execution_manifest_sha256}")

# %% [markdown]
# ## Checks e próximo passo
#
# A Bronze está aprovada somente se todos os uploads originais tiverem bytes e
# SHA-256 remotos reconciliados, as quatro tabelas tiverem as contagens esperadas
# e o manifesto de execução terminar em `PASS`. A próxima etapa é construir o
# `source()` dbt e os modelos staging/Silver; este notebook não executa limpeza,
# labels, features nem treinamento.

# %%
print("=" * 72)
print("BRONZE: PASS")
print(f"Arquivos originais reconciliados: {len(source_uploads)}")
print(f"Tabelas reconciliadas           : {len(table_results)}")
print(f"Telemetria                      : {table_results['telemetria_raw']['rows']:,}")
print(f"Apontamentos                    : {table_results['apontamentos_raw']['rows']:,}")
print(f"CMA                             : {table_results['cma_regras_raw']['rows']:,}")
print(f"Dicionário                      : {table_results['dicionario_dados_raw']['rows']:,}")
print("=" * 72)
