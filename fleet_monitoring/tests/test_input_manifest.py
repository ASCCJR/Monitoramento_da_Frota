import json
from pathlib import Path

from fleet_monitoring.src.input_manifest import validate_manifest


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "fleet_monitoring/manifests/inputs_v1.json"


def test_manifest_uses_only_canonical_inputs() -> None:
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    paths = {item["path"] for item in data["inputs"]}

    assert len(paths) == 9
    assert "docs/business/Alarmes - Regra de Negocio_V2.xlsx" in paths
    assert "docs/business/Dicionario_Dados.xlsx" in paths
    assert not any("alarmes_regras_negocio.xlsx" in path for path in paths)
    assert not any("dicionario_dados.xlsx" in path for path in paths)


def test_manifest_reconciles_files() -> None:
    checks = validate_manifest(MANIFEST, REPO_ROOT)

    assert len(checks) == 36
    assert all(check["status"] == "PASS" for check in checks)
