"""Tests for manifest creation, validation, date extraction, and Databento manifest loading."""

import json
import tempfile
from pathlib import Path

from databento_ingest.manifest import (
    MANIFEST_SCHEMA_VERSION,
    create_manifest,
    extract_date_range,
    read_manifest,
    validate_manifest,
)
from databento_ingest.downloader import load_databento_manifest


def _minimal_manifest() -> dict:
    """Return a minimal valid manifest dict."""
    return {
        "schema_version": "1.3",
        "symbol": "NVDA",
        "dataset": "OPRA",
        "source": "databento",
        "download_method": "https",
        "date_range": "2025-11-13 to 2025-11-25",
        "download_timestamp": "2026-03-06T12:00:00+00:00",
        "file_count": 2,
        "files": ["file_a.dbn.zst", "file_b.dbn.zst"],
        "checksums": {},
        "metadata": {},
    }


class TestValidateManifest:
    def test_valid_manifest(self):
        m = _minimal_manifest()
        errors = validate_manifest(m)
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_missing_required_field(self):
        for field in [
            "schema_version", "symbol", "dataset", "source",
            "download_method", "date_range", "download_timestamp",
            "file_count", "files",
        ]:
            m = _minimal_manifest()
            del m[field]
            errors = validate_manifest(m)
            assert any(field in e for e in errors), (
                f"Expected error for missing '{field}', got: {errors}"
            )

    def test_wrong_type(self):
        m = _minimal_manifest()
        m["file_count"] = "not_an_int"
        errors = validate_manifest(m)
        assert any("file_count" in e and "int" in e for e in errors), (
            f"Expected type error for file_count, got: {errors}"
        )

    def test_file_count_mismatch(self):
        m = _minimal_manifest()
        m["file_count"] = 999
        errors = validate_manifest(m)
        assert any("file_count" in e and "does not match" in e for e in errors), (
            f"Expected file_count mismatch error, got: {errors}"
        )

    def test_checksums_wrong_type(self):
        m = _minimal_manifest()
        m["checksums"] = "not_a_dict"
        errors = validate_manifest(m)
        assert any("checksums" in e for e in errors), (
            f"Expected checksums type error, got: {errors}"
        )

    def test_schema_version_constant(self):
        assert MANIFEST_SCHEMA_VERSION == "1.3", (
            f"Expected MANIFEST_SCHEMA_VERSION='1.3', got '{MANIFEST_SCHEMA_VERSION}'"
        )


class TestCreateManifest:
    def test_creates_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = create_manifest(
                output_dir=Path(tmpdir),
                symbol="NVDA",
                source="https",
                date_range="2025-11-13 to 2025-11-25",
                files=["a.dbn.zst", "b.dbn.zst"],
                metadata={"job_id": "TEST-123"},
                dataset="OPRA",
                schema="cmbp-1",
                checksums={"a.dbn.zst": "abc123"},
            )

            assert path.exists(), f"Manifest file should exist at {path}"
            data = json.loads(path.read_text())
            assert data["schema_version"] == "1.3", f"Expected schema_version='1.3', got '{data['schema_version']}'"
            assert data["symbol"] == "NVDA", f"Expected symbol='NVDA', got '{data['symbol']}'"
            assert data["dataset"] == "OPRA", f"Expected dataset='OPRA', got '{data['dataset']}'"
            assert data["schema"] == "cmbp-1", f"Expected schema='cmbp-1', got '{data['schema']}'"
            assert data["file_count"] == 2, f"Expected file_count=2, got {data['file_count']}"
            assert data["files"] == ["a.dbn.zst", "b.dbn.zst"], f"Expected sorted files, got {data['files']}"
            assert data["checksums"] == {"a.dbn.zst": "abc123"}, f"Expected checksums dict, got {data['checksums']}"
            assert data["download_method"] == "https", f"Expected download_method='https', got '{data['download_method']}'"

            errors = validate_manifest(data)
            assert errors == [], f"Created manifest should be valid, got: {errors}"

    def test_files_are_sorted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = create_manifest(
                output_dir=Path(tmpdir),
                symbol="NVDA",
                source="https",
                date_range="test",
                files=["z.dbn.zst", "a.dbn.zst", "m.dbn.zst"],
                metadata={},
            )
            data = json.loads(path.read_text())
            assert data["files"] == ["a.dbn.zst", "m.dbn.zst", "z.dbn.zst"], (
                f"Expected sorted files, got {data['files']}"
            )

    def test_schema_omitted_when_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = create_manifest(
                output_dir=Path(tmpdir),
                symbol="NVDA",
                source="https",
                date_range="test",
                files=["a.dbn.zst"],
                metadata={},
            )
            data = json.loads(path.read_text())
            assert "schema" not in data, f"Expected no 'schema' key when None, got '{data.get('schema')}'"

    def test_traceability_metadata_preserved(self):
        """Schema v1.3 contract: ingest_tool_version and databento_api_version
        are preserved when written to manifest. Documents the multi-year
        reproducibility contract added in Round 2 fixes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = create_manifest(
                output_dir=Path(tmpdir),
                symbol="NVDA",
                source="https",
                date_range="test",
                files=["a.dbn.zst"],
                metadata={
                    "ingest_tool_version": "0.2.0",
                    "databento_api_version": 0,
                    "job_id": "TEST-123",
                    "total_size_bytes": 1000,
                    "failed_files": [],
                    "download_speed_mbps": 35.1,
                    "download_elapsed_seconds": 10.0,
                    "parallel_connections": 4,
                },
            )
            data = json.loads(path.read_text())
            md = data["metadata"]
            assert md["ingest_tool_version"] == "0.2.0", (
                f"Expected ingest_tool_version='0.2.0', got '{md.get('ingest_tool_version')}'"
            )
            assert md["databento_api_version"] == 0, (
                f"Expected databento_api_version=0, got {md.get('databento_api_version')}"
            )
            assert md["failed_files"] == [], (
                f"Expected failed_files=[], got {md.get('failed_files')}"
            )
            assert md["download_elapsed_seconds"] == 10.0, (
                f"Expected download_elapsed_seconds=10.0, got {md.get('download_elapsed_seconds')}"
            )

    def test_no_orphan_tmp_on_success(self):
        """Atomic write contract: after successful create_manifest, only
        manifest.json exists (the .tmp temp file is renamed away)."""
        import os
        with tempfile.TemporaryDirectory() as tmpdir:
            create_manifest(
                output_dir=Path(tmpdir),
                symbol="NVDA",
                source="https",
                date_range="test",
                files=["a.dbn.zst"],
                metadata={},
            )
            files_in_dir = set(os.listdir(tmpdir))
            assert files_in_dir == {"manifest.json"}, (
                f"Expected only manifest.json after success, got {files_in_dir}"
            )

    def test_orphan_tmp_cleaned_on_failure(self):
        """Atomic write contract: if json.dump raises (non-serializable
        metadata), the .tmp file must be cleaned up — not orphaned on disk."""
        import os
        with tempfile.TemporaryDirectory() as tmpdir:
            # Pass a non-JSON-serializable value (a set) inside metadata
            try:
                create_manifest(
                    output_dir=Path(tmpdir),
                    symbol="NVDA",
                    source="https",
                    date_range="test",
                    files=["a.dbn.zst"],
                    metadata={"bad_value": {1, 2, 3}},  # set is not JSON-serializable
                )
                assert False, "Expected TypeError from json.dump on non-serializable metadata"
            except TypeError:
                pass  # Expected
            files_in_dir = set(os.listdir(tmpdir))
            assert files_in_dir == set(), (
                f"Expected no files after failed create_manifest (cleanup), got {files_in_dir}"
            )


class TestReadManifest:
    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = create_manifest(
                output_dir=Path(tmpdir),
                symbol="NVDA",
                source="https",
                date_range="test",
                files=["a.dbn.zst"],
                metadata={"key": "value"},
            )
            data = read_manifest(path)
            assert data["symbol"] == "NVDA", f"Expected symbol='NVDA', got '{data['symbol']}'"
            assert data["metadata"]["key"] == "value", f"Expected metadata key='value', got '{data['metadata'].get('key')}'"


class TestExtractDateRange:
    def test_xnas_filenames(self):
        filenames = [
            "xnas-itch-20250929.mbo.dbn.zst",
            "xnas-itch-20251015.mbo.dbn.zst",
        ]
        result = extract_date_range(filenames)
        assert result == "2025-09-29 to 2025-10-15", f"Expected '2025-09-29 to 2025-10-15', got '{result}'"

    def test_opra_filenames(self):
        filenames = [
            "opra-pillar-20251113.cmbp-1.dbn.zst",
            "opra-pillar-20251125.cmbp-1.dbn.zst",
        ]
        result = extract_date_range(filenames)
        assert result == "2025-11-13 to 2025-11-25", f"Expected '2025-11-13 to 2025-11-25', got '{result}'"

    def test_single_file(self):
        result = extract_date_range(["xnas-itch-20250101.mbo.dbn.zst"])
        assert result == "2025-01-01 to 2025-01-01", f"Expected same start/end for single file, got '{result}'"

    def test_no_dates(self):
        result = extract_date_range(["readme.txt"])
        assert result == "unknown", f"Expected 'unknown' for no-date filename, got '{result}'"

    def test_empty_list(self):
        result = extract_date_range([])
        assert result == "unknown", f"Expected 'unknown' for empty list, got '{result}'"

    def test_mixed_datasets(self):
        filenames = [
            "opra-pillar-20251125.cmbp-1.dbn.zst",
            "xnas-itch-20250101.mbo.dbn.zst",
        ]
        result = extract_date_range(filenames)
        assert result == "2025-01-01 to 2025-11-25", f"Expected '2025-01-01 to 2025-11-25', got '{result}'"


class TestLoadDatabentoManifest:
    def test_loads_dbn_zst_files_only(self):
        manifest = {
            "job_id": "TEST-123",
            "files": [
                {
                    "filename": "condition.json",
                    "size": 100,
                    "hash": "sha256:abc",
                    "urls": {"https": "https://example.com/condition.json"},
                },
                {
                    "filename": "data-20251113.cmbp-1.dbn.zst",
                    "size": 30000000000,
                    "hash": "sha256:def",
                    "urls": {"https": "https://example.com/data.dbn.zst"},
                },
            ],
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(manifest, f)
            f.flush()
            tmp_path = Path(f.name)
        try:
            result = load_databento_manifest(tmp_path)
            assert len(result) == 1, f"Expected 1 .dbn.zst file, got {len(result)}"
            assert result[0]["filename"] == "data-20251113.cmbp-1.dbn.zst", (
                f"Expected 'data-20251113.cmbp-1.dbn.zst', got '{result[0]['filename']}'"
            )
            assert result[0]["size"] == 30000000000, f"Expected size=30000000000, got {result[0]['size']}"
            assert result[0]["hash"] == "sha256:def", f"Expected hash='sha256:def', got '{result[0]['hash']}'"
            assert result[0]["https_url"] == "https://example.com/data.dbn.zst", (
                f"Expected URL 'https://example.com/data.dbn.zst', got '{result[0]['https_url']}'"
            )
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_raises_on_missing_files_key(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"job_id": "TEST"}, f)
            f.flush()
            try:
                load_databento_manifest(Path(f.name))
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "missing 'files'" in str(e)
            finally:
                Path(f.name).unlink()

    def test_raises_on_missing_https_url(self):
        manifest = {
            "files": [
                {
                    "filename": "test.dbn.zst",
                    "size": 100,
                    "hash": "sha256:abc",
                    "urls": {"ftp": "ftp://example.com/test.dbn.zst"},
                },
            ],
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(manifest, f)
            f.flush()
            try:
                load_databento_manifest(Path(f.name))
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "No HTTPS URL" in str(e)
            finally:
                Path(f.name).unlink()
