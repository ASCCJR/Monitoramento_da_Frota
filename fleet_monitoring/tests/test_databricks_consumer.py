import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RENDERER = ROOT / "scripts" / "render_databricks_consumer.py"
SCORING_ID = "b" * 64


def test_render_consumption_artifacts(tmp_path, monkeypatch):
    monkeypatch.setenv("FLEET_CATALOG", "portfolio_reader")
    monkeypatch.setenv("FLEET_SILVER_SCHEMA", "source_silver")
    monkeypatch.setenv("FLEET_GOLD_SCHEMA", "source_gold")
    monkeypatch.setenv("FLEET_PRODUCT_SCHEMA", "reader_product")
    command = [
        sys.executable,
        str(RENDERER),
        "--scoring-run-id",
        SCORING_ID,
        "--reference-time",
        "2025-06-15 09:00:00",
        "--output-dir",
        str(tmp_path),
    ]
    subprocess.run(command, check=True)

    sql = sorted(tmp_path.glob("*.sql"))
    assert [file.name for file in sql] == [
        "01_bootstrap.sql",
        "02_publish_snapshot.sql",
        "03_equipment_events.sql",
        "04_fleet_investigation.sql",
        "05_fleet_exploration_history.sql",
        "06_equipment_event_history.sql",
        "07_validate_consumption.sql",
    ]
    bootstrap, publish, *_, validate = [file.read_text() for file in sql]
    assert f"s.scoring_run_id = '{SCORING_ID}'" in bootstrap
    assert "`portfolio_reader`.`source_silver`.int_scoring_grade" in bootstrap
    assert "`portfolio_reader`.`source_gold`.fct_operational_scores" in bootstrap
    assert "`portfolio_reader`.`reader_product`.replay_head" in publish
    snapshot = hashlib.sha256(f"{SCORING_ID}:2025-06-15 09:00:00".encode()).hexdigest()
    assert snapshot in publish and snapshot in validate
    assert "BATCH_SCORE_SNAPSHOT" in publish
    assert "replay_batches" not in publish

    genie = json.loads((tmp_path / "acompanhamento_frota.genie.json").read_text())
    identifiers = [item["identifier"] for item in genie["data_sources"]["tables"]]
    assert len(identifiers) == 6 and all(
        name.startswith("portfolio_reader.reader_product.") for name in identifiers
    )
    for example in genie["instructions"]["example_question_sqls"]:
        assert "`portfolio_reader`.`reader_product`." in " ".join(example["sql"])
    for benchmark in genie["benchmarks"]["questions"]:
        assert "`portfolio_reader`.`reader_product`." in " ".join(
            benchmark["answer"][0]["content"]
        )

    dashboard_path = tmp_path / "acompanhamento_frota.lvdash.json"
    dashboard = json.loads(dashboard_path.read_text())
    assert len(dashboard["datasets"]) == 13
    assert "genieSpace" not in dashboard["uiSettings"]
    overview = next(
        page for page in dashboard["pages"] if page["displayName"] == "Acompanhamento"
    )
    widgets = {item["widget"]["name"]: item["widget"] for item in overview["layout"]}
    for name in ("investigation-equipment", "priorities", "investigation-evidence"):
        assert widgets[name]["queries"][0]["query"]["datasetName"] == "priorities"
    assert (
        widgets["investigation-equipment"]["spec"]["encodings"]["fields"][0][
            "fieldName"
        ]
        == "tag"
    )
    genie_id = "a" * 32
    subprocess.run([*command, "--genie-space-id", genie_id], check=True)
    dashboard = json.loads(dashboard_path.read_text())
    assert dashboard["uiSettings"]["genieSpace"]["overrideId"] == genie_id

    for file in tmp_path.iterdir():
        content = file.read_text()
        assert "{{" not in content
        assert "workspace.dv_v2" not in content
        assert "dbc-" not in content


def test_rejects_unsafe_namespace(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            str(RENDERER),
            "--catalog",
            "catalog`; SELECT 1; --",
            "--scoring-run-id",
            SCORING_ID,
            "--output-dir",
            str(tmp_path),
        ],
        capture_output=True,
    )
    assert result.returncode == 2
    assert not list(tmp_path.iterdir())
