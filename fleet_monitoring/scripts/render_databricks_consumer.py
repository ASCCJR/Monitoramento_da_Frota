"""Prepare o SQL e as configurações do dashboard e do Genie para um workspace."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_FILES = (
    "bootstrap.sql",
    "publish_snapshot.sql",
    "equipment_events.sql",
    "fleet_investigation.sql",
    "fleet_exploration_history.sql",
    "equipment_event_history.sql",
    "validate_consumption.sql",
)
SLOT = re.compile(r"\{\{([a-z_]+)\}\}")


def sql_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", value):
        raise argparse.ArgumentTypeError(
            "use letras, números e sublinhados, começando por letra ou sublinhado"
        )
    return value


def scoring_run_id(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise argparse.ArgumentTypeError("use o SHA-256 retornado pelo scoring batch")
    return value


def reference_hour(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("use o formato AAAA-MM-DD HH:MM:SS") from error
    if (
        parsed.tzinfo is not None
        or parsed.minute
        or parsed.second
        or parsed.microsecond
    ):
        raise argparse.ArgumentTypeError("use uma hora cheia sem fuso horário")
    if not datetime(2025, 6, 1) <= parsed < datetime(2025, 7, 1):
        raise argparse.ArgumentTypeError(
            "use uma hora do período de scoring em junho de 2025"
        )
    return parsed.isoformat(sep=" ", timespec="seconds")


def genie_space_id(value: str) -> str:
    try:
        identifier = uuid.UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "use o ID do Genie criado no seu workspace"
        ) from error
    if value not in {identifier.hex, str(identifier)}:
        raise argparse.ArgumentTypeError("use o ID exato retornado pelo Genie")
    return value


def render(template: str, values: dict[str, str]) -> str:
    return SLOT.sub(lambda slot: values[slot.group(1)], template)


def write_json(path: Path, content: dict) -> None:
    path.write_text(
        json.dumps(content, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog", type=sql_identifier, default=os.getenv("FLEET_CATALOG")
    )
    for layer in ("silver", "gold", "product"):
        parser.add_argument(
            f"--{layer}-schema",
            type=sql_identifier,
            default=os.getenv(f"FLEET_{layer.upper()}_SCHEMA", f"fleet_{layer}"),
        )
    parser.add_argument("--scoring-run-id", required=True, type=scoring_run_id)
    parser.add_argument(
        "--reference-time", type=reference_hour, default="2025-06-15 10:00:00"
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--genie-space-id", type=genie_space_id)
    args = parser.parse_args()
    if args.catalog is None:
        parser.error("defina --catalog ou FLEET_CATALOG")
    if (
        len(
            {
                args.silver_schema.lower(),
                args.gold_schema.lower(),
                args.product_schema.lower(),
            }
        )
        != 3
    ):
        parser.error("use schemas distintos para Silver, Gold e produto")

    snapshot_key = f"{args.scoring_run_id}:{args.reference_time}"
    values = {
        "silver_namespace": f"`{args.catalog}`.`{args.silver_schema}`",
        "gold_namespace": f"`{args.catalog}`.`{args.gold_schema}`",
        "product_namespace": f"`{args.catalog}`.`{args.product_schema}`",
        "scoring_run_id": args.scoring_run_id,
        "reference_time": args.reference_time,
        "snapshot_id": hashlib.sha256(snapshot_key.encode()).hexdigest(),
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for order, name in enumerate(SQL_FILES, start=1):
        template = (ROOT / "demo" / "sql" / name).read_text(encoding="utf-8")
        (args.output_dir / f"{order:02}_{name}").write_text(
            render(template, values), encoding="utf-8"
        )

    genie_file = "acompanhamento_frota.genie.json"
    genie_template = (ROOT / "genie" / genie_file).read_text(encoding="utf-8")
    genie = json.loads(render(genie_template, values))
    for table in genie["data_sources"]["tables"]:
        name = table["identifier"].rsplit(".", 1)[1]
        table["identifier"] = f"{args.catalog}.{args.product_schema}.{name}"
    write_json(args.output_dir / genie_file, genie)

    dashboard_file = "acompanhamento_frota.lvdash.json"
    dashboard = json.loads(
        (ROOT / "dashboards" / dashboard_file).read_text(encoding="utf-8")
    )
    if args.genie_space_id:
        dashboard["uiSettings"]["genieSpace"] = {
            "isEnabled": True,
            "overrideId": args.genie_space_id,
            "enablementMode": "ENABLED",
        }
    write_json(args.output_dir / dashboard_file, dashboard)


if __name__ == "__main__":
    main()
