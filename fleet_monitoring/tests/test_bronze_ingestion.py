import io
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fleet_monitoring.src.bronze_ingestion import (
    ensure_spark_timestamp_precision,
    source_volume_path,
    upload_immutable_file,
    validate_identifier,
)
from fleet_monitoring.src.input_manifest import sha256_file


class NotFound(Exception):
    pass


class FakeFilesApi:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def get_metadata(self, path: str) -> SimpleNamespace:
        if path not in self.files:
            raise NotFound(path)
        return SimpleNamespace(content_length=len(self.files[path]))

    def create_directory(self, path: str) -> None:
        del path

    def upload(self, path: str, contents: io.BufferedReader, *, overwrite: bool) -> None:
        if path in self.files and not overwrite:
            raise FileExistsError(path)
        self.files[path] = contents.read()

    def download(self, path: str) -> SimpleNamespace:
        return SimpleNamespace(contents=io.BytesIO(self.files[path]))


def test_identifiers_accept_reader_catalog_and_schema() -> None:
    validate_identifier("reader_catalog")
    validate_identifier("fleet_bronze")

    with pytest.raises(ValueError, match="Identificador inválido"):
        validate_identifier("fleet.bronze")
    with pytest.raises(ValueError, match="Identificador inválido"):
        validate_identifier("catalog name")


def test_source_path_is_versioned_and_content_addressed() -> None:
    item = {
        "path": "data/telemetria/telemetry_jan.parquet",
        "role": "raw_telemetria",
        "sha256": "a" * 64,
    }

    actual = source_volume_path(
        "/Volumes/workspace/dv_v2_bronze/raw_inputs",
        "1.0.0",
        item,
    )

    assert actual == (
        "/Volumes/workspace/dv_v2_bronze/raw_inputs/manifest_version=1.0.0/"
        f"originals/raw_telemetria/{'a' * 64}/telemetry_jan.parquet"
    )


def test_upload_is_immutable_and_reconciles_remote_hash(tmp_path: Path) -> None:
    local_path = tmp_path / "source.bin"
    local_path.write_bytes(b"conteudo-versionado")
    expected_sha256 = sha256_file(local_path)
    remote_path = "/Volumes/workspace/dv_v2_bronze/raw_inputs/source.bin"
    files_api = FakeFilesApi()

    first = upload_immutable_file(
        files_api,
        local_path,
        remote_path,
        expected_sha256,
        local_path.stat().st_size,
    )
    second = upload_immutable_file(
        files_api,
        local_path,
        remote_path,
        expected_sha256,
        local_path.stat().st_size,
    )

    assert first["status"] == "UPLOADED"
    assert second["status"] == "REUSED"
    assert second["sha256"] == expected_sha256


def test_timestamp_conversion_preserves_original_and_rows(tmp_path: Path) -> None:
    source = tmp_path / "nanos.parquet"
    source_table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "event_time": pa.array([1_000, 2_000], type=pa.timestamp("ns")),
        }
    )
    pq.write_table(source_table, source)
    source_sha256 = sha256_file(source)

    converted = ensure_spark_timestamp_precision(source, tmp_path / "prepared")

    assert converted != source
    assert pq.read_schema(source).field("event_time").type.unit == "ns"
    assert pq.read_schema(converted).field("event_time").type.unit == "us"
    assert pq.ParquetFile(converted).metadata.num_rows == 2
    assert sha256_file(source) == source_sha256


def test_timestamp_conversion_fails_on_real_nanosecond_loss(tmp_path: Path) -> None:
    source = tmp_path / "unsafe_nanos.parquet"
    pq.write_table(
        pa.table({"event_time": pa.array([1], type=pa.timestamp("ns"))}),
        source,
    )

    with pytest.raises(pa.ArrowInvalid):
        ensure_spark_timestamp_precision(source, tmp_path / "prepared")
