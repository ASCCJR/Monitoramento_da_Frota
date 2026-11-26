"""Helpers determinísticos para a ingestão Bronze."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from fleet_monitoring.src.input_manifest import sha256_file


SAFE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_identifier(name: str) -> None:
    """Verifica nomes interpolados no SQL e nos caminhos de Volume."""
    if not SAFE_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Identificador inválido: {name}")


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Carrega o manifesto versionado e valida sua estrutura mínima."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    required = {"manifest_version", "contract_version", "inputs", "expected_totals"}
    missing = required.difference(manifest)
    if missing:
        raise ValueError(f"Manifesto incompleto; campos ausentes: {sorted(missing)}")
    if not manifest["inputs"]:
        raise ValueError("Manifesto sem inputs")
    return manifest


def source_volume_path(volume_root: str, manifest_version: str, item: dict[str, Any]) -> str:
    """Monta caminho imutável e endereçado pelo hash do arquivo original."""
    filename = Path(item["path"]).name
    return (
        f"{volume_root}/manifest_version={manifest_version}/originals/"
        f"{item['role']}/{item['sha256']}/{filename}"
    )


def manifest_volume_path(volume_root: str, manifest_version: str) -> str:
    """Retorna o caminho remoto do manifesto que governa a ingestão."""
    return (
        f"{volume_root}/manifest_version={manifest_version}/manifests/"
        "inputs_v1.json"
    )


def prepared_volume_path(
    volume_root: str,
    manifest_version: str,
    role: str,
    sha256: str,
    filename: str,
) -> str:
    """Monta caminho de um derivado técnico, também endereçado por conteúdo."""
    return (
        f"{volume_root}/manifest_version={manifest_version}/prepared/"
        f"{role}/{sha256}/{filename}"
    )


def ensure_spark_timestamp_precision(source: Path, target_dir: Path) -> Path:
    """Converte TIMESTAMP(NANOS) para MICROS sem sobrescrever o arquivo original.

    O cast usa ``allow_time_truncate=False``. Assim, qualquer valor que perderia
    precisão causa erro em vez de ser truncado silenciosamente.
    """
    source_schema = pq.read_schema(source)
    needs_conversion = any(
        pa.types.is_timestamp(field.type) and field.type.unit == "ns"
        for field in source_schema
    )
    if not needs_conversion:
        return source

    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{source.stem}__timestamp_us.parquet"
    table = pq.read_table(source)
    arrays: list[pa.ChunkedArray] = []
    fields: list[pa.Field] = []

    for field, column in zip(table.schema, table.columns, strict=True):
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            target_type = pa.timestamp("us", tz=field.type.tz)
            options = pc.CastOptions(target_type, allow_time_truncate=False)
            arrays.append(pc.cast(column, options=options))
            fields.append(pa.field(field.name, target_type, nullable=field.nullable))
        else:
            arrays.append(column)
            fields.append(field)

    converted = pa.Table.from_arrays(
        arrays,
        schema=pa.schema(fields, metadata=table.schema.metadata),
    )
    pq.write_table(converted, target)

    if pq.ParquetFile(target).metadata.num_rows != pq.ParquetFile(source).metadata.num_rows:
        raise RuntimeError("A conversão de precisão alterou a contagem de linhas")
    return target


def remote_sha256(files_api: Any, remote_path: str, chunk_size: int = 1024 * 1024) -> str:
    """Calcula o SHA-256 do conteúdo já persistido em um Volume UC."""
    response = files_api.download(remote_path)
    stream = response.contents
    digest = hashlib.sha256()
    try:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    finally:
        close = getattr(stream, "close", None)
        if close is not None:
            close()
    return digest.hexdigest()


def _is_not_found(error: Exception) -> bool:
    """Reconhece respostas de ausência sem ocultar outras falhas da API."""
    error_code = str(getattr(error, "error_code", ""))
    status_code = getattr(error, "http_status_code", None)
    return (
        error.__class__.__name__ in {"NotFound", "ResourceDoesNotExist"}
        or error_code in {"NOT_FOUND", "RESOURCE_DOES_NOT_EXIST"}
        or status_code == 404
    )


def upload_immutable_file(
    files_api: Any,
    local_path: Path,
    remote_path: str,
    expected_sha256: str,
    expected_bytes: int,
) -> dict[str, Any]:
    """Faz upload sem overwrite e reconcilia tamanho e hash no destino."""
    actual_local_sha256 = sha256_file(local_path)
    actual_local_bytes = local_path.stat().st_size
    if actual_local_sha256 != expected_sha256:
        raise ValueError(
            f"Hash local divergente em {local_path}: "
            f"esperado={expected_sha256}, atual={actual_local_sha256}"
        )
    if actual_local_bytes != expected_bytes:
        raise ValueError(
            f"Tamanho local divergente em {local_path}: "
            f"esperado={expected_bytes}, atual={actual_local_bytes}"
        )

    existed = True
    try:
        metadata = files_api.get_metadata(remote_path)
    except Exception as error:  # noqa: BLE001 - restringido por _is_not_found
        if not _is_not_found(error):
            raise
        existed = False
        files_api.create_directory(remote_path.rsplit("/", 1)[0])
        with local_path.open("rb") as source:
            files_api.upload(remote_path, source, overwrite=False)
        metadata = files_api.get_metadata(remote_path)

    remote_bytes = int(metadata.content_length)
    if remote_bytes != expected_bytes:
        raise RuntimeError(
            f"Tamanho remoto divergente em {remote_path}: "
            f"esperado={expected_bytes}, atual={remote_bytes}"
        )
    actual_remote_sha256 = remote_sha256(files_api, remote_path)
    if actual_remote_sha256 != expected_sha256:
        raise RuntimeError(
            f"Hash remoto divergente em {remote_path}: "
            f"esperado={expected_sha256}, atual={actual_remote_sha256}"
        )

    return {
        "local_path": str(local_path),
        "remote_path": remote_path,
        "bytes": remote_bytes,
        "sha256": actual_remote_sha256,
        "status": "REUSED" if existed else "UPLOADED",
    }


def git_snapshot(repo_root: Path) -> dict[str, Any]:
    """Registra commit e estado sujo sem incluir conteúdo ou diff do usuário."""
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status_lines = subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    return {
        "commit": commit,
        "worktree_dirty": bool(status_lines),
        "changed_path_count": len(status_lines),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Persiste um artefato JSON de execução com escrita atômica local."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
