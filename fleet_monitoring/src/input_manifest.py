"""Validação reproduzível do manifesto de inputs públicos da v2."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from openpyxl import load_workbook


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calcula SHA-256 em streaming, sem carregar o arquivo inteiro."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_records(path: Path, item: dict[str, Any]) -> int:
    """Conta registros sem ler payloads parquet completos."""
    if item["format"] == "parquet":
        return pq.ParquetFile(path).metadata.num_rows

    workbook = load_workbook(path, read_only=True, data_only=True)
    if "sheet" in item:
        return max(workbook[item["sheet"]].max_row - 1, 0)
    return sum(max(workbook[sheet].max_row - 1, 0) for sheet in item["sheets"])


def validate_manifest(manifest_path: Path, repo_root: Path) -> list[dict[str, Any]]:
    """Retorna checks textuais de existência, tamanho, contagem e hash."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    checks: list[dict[str, Any]] = []
    for item in manifest["inputs"]:
        path = repo_root / item["path"]
        exists = path.is_file()
        actual_bytes = path.stat().st_size if exists else None
        actual_records = count_records(path, item) if exists else None
        actual_sha256 = sha256_file(path) if exists else None
        checks.extend(
            [
                {
                    "path": item["path"],
                    "check": "exists",
                    "expected": True,
                    "actual": exists,
                    "status": "PASS" if exists else "FAIL",
                },
                {
                    "path": item["path"],
                    "check": "bytes",
                    "expected": item["bytes"],
                    "actual": actual_bytes,
                    "status": "PASS" if actual_bytes == item["bytes"] else "FAIL",
                },
                {
                    "path": item["path"],
                    "check": "records",
                    "expected": item["records"],
                    "actual": actual_records,
                    "status": "PASS" if actual_records == item["records"] else "FAIL",
                },
                {
                    "path": item["path"],
                    "check": "sha256",
                    "expected": item["sha256"],
                    "actual": actual_sha256,
                    "status": "PASS" if actual_sha256 == item["sha256"] else "FAIL",
                },
            ]
        )
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("fleet_monitoring/manifests/inputs_v1.json"),
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args()

    checks = validate_manifest(args.manifest, args.repo_root)
    for check in checks:
        print(
            f"[{check['status']}] {check['path']} :: {check['check']} "
            f"expected={check['expected']} actual={check['actual']}"
        )
    passed = all(check["status"] == "PASS" for check in checks)
    print(f"INPUT MANIFEST: {'PASS' if passed else 'FAIL'} ({len(checks)} checks)")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
