"""Tests for config loading, validation, and credential management."""

import tempfile
from pathlib import Path

from databento_ingest.config import (
    Credentials,
    DatasetConfig,
    DownloadParams,
    IngestConfig,
    SourceConfig,
    StorageConfig,
    load_credentials,
    validate_config,
)


def _valid_config() -> IngestConfig:
    """Return a minimal valid IngestConfig for testing."""
    return IngestConfig(
        source=SourceConfig(method="https", job_id="TEST-20260101-ABC123"),
        dataset=DatasetConfig(name="OPRA", symbol="NVDA"),
        storage=StorageConfig(output_dir="data/test"),
        download=DownloadParams(parallel=2),
    )


class TestValidateConfig:
    def test_valid_config_passes(self):
        config = _valid_config()
        errors = validate_config(config)
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_invalid_method(self):
        config = _valid_config()
        config.source.method = "ftp"
        errors = validate_config(config)
        assert any("source.method" in e for e in errors), f"Expected method error, got: {errors}"

    def test_only_https_allowed(self):
        config = _valid_config()
        config.source.method = "http"
        errors = validate_config(config)
        assert any("https" in e for e in errors), f"Expected https error, got: {errors}"

    def test_job_id_required_without_manifest(self):
        config = _valid_config()
        config.source.job_id = ""
        config.source.manifest_path = ""
        errors = validate_config(config)
        assert any("job_id" in e or "manifest_path" in e for e in errors), (
            f"Expected job_id or manifest_path error, got: {errors}"
        )

    def test_manifest_path_sufficient_without_job_id(self):
        config = _valid_config()
        config.source.job_id = ""
        config.source.manifest_path = "path/to/manifest.json"
        errors = validate_config(config)
        source_errors = [e for e in errors if "job_id" in e or "manifest_path" in e]
        assert source_errors == [], (
            f"manifest_path should satisfy requirement, got: {source_errors}"
        )

    def test_missing_dataset_name(self):
        config = _valid_config()
        config.dataset.name = ""
        errors = validate_config(config)
        assert any("dataset.name" in e for e in errors), f"Expected name error, got: {errors}"

    def test_missing_symbol(self):
        config = _valid_config()
        config.dataset.symbol = ""
        errors = validate_config(config)
        assert any("dataset.symbol" in e for e in errors), f"Expected symbol error, got: {errors}"

    def test_missing_output_dir(self):
        config = _valid_config()
        config.storage.output_dir = ""
        errors = validate_config(config)
        assert any("storage.output_dir" in e for e in errors), (
            f"Expected output_dir error, got: {errors}"
        )

    def test_parallel_zero(self):
        config = _valid_config()
        config.download.parallel = 0
        errors = validate_config(config)
        assert any("parallel" in e and ">= 1" in e for e in errors), (
            f"Expected parallel >= 1 error, got: {errors}"
        )

    def test_parallel_too_high(self):
        config = _valid_config()
        config.download.parallel = 10
        errors = validate_config(config)
        assert any("parallel" in e and "<= 8" in e for e in errors), (
            f"Expected parallel <= 8 error, got: {errors}"
        )

    def test_parallel_valid_range(self):
        for n in (1, 2, 4, 8):
            config = _valid_config()
            config.download.parallel = n
            errors = validate_config(config)
            parallel_errors = [e for e in errors if "parallel" in e]
            assert parallel_errors == [], (
                f"parallel={n} should be valid, got: {parallel_errors}"
            )


class TestLoadCredentials:
    def test_missing_file_returns_empty(self):
        creds = load_credentials(Path("/nonexistent/path/credentials.toml"))
        assert creds.api_key == "", f"Expected empty api_key for missing file, got '{creds.api_key}'"

    def test_loads_api_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[databento]\napi_key = "db-TEST123"\n')
            f.flush()
            tmp_path = Path(f.name)
        try:
            creds = load_credentials(tmp_path)
            assert creds.api_key == "db-TEST123", f"Expected 'db-TEST123', got '{creds.api_key}'"
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_empty_section_returns_empty(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write("[databento]\n")
            f.flush()
            tmp_path = Path(f.name)
        try:
            creds = load_credentials(tmp_path)
            assert creds.api_key == "", f"Expected empty api_key for empty section, got '{creds.api_key}'"
        finally:
            tmp_path.unlink(missing_ok=True)


class TestDefaults:
    def test_download_params_defaults(self):
        params = DownloadParams()
        assert params.parallel == 2, f"Expected parallel=2, got {params.parallel}"

    def test_dataset_config_defaults(self):
        ds = DatasetConfig(name="TEST")
        assert ds.symbol == "NVDA", f"Expected symbol='NVDA', got '{ds.symbol}'"
        assert ds.schema == "", f"Expected empty schema, got '{ds.schema}'"

    def test_credentials_defaults(self):
        creds = Credentials()
        assert creds.api_key == "", f"Expected empty api_key, got '{creds.api_key}'"

    def test_source_config_defaults(self):
        src = SourceConfig()
        assert src.method == "https", f"Expected method='https', got '{src.method}'"
        assert src.job_id == "", f"Expected empty job_id, got '{src.job_id}'"
        assert src.manifest_path == "", f"Expected empty manifest_path, got '{src.manifest_path}'"
