"""TOML configuration loader for dataset downloads and credentials.

Two config layers:
    1. Dataset config (configs/datasets/*.toml): defines what to download
       (job ID, dataset/schema/symbol, storage path, download params).
    2. Credentials (credentials.toml, gitignored): Databento API key.
       CLI args override credentials.toml values.

Config validation fails fast with precise errors per RULE.md §5.
"""

from dataclasses import dataclass, field
from pathlib import Path

import tomllib

MODULE_ROOT = Path(__file__).resolve().parent.parent.parent
CREDENTIALS_PATH = MODULE_ROOT / "credentials.toml"
CONFIGS_DIR = MODULE_ROOT / "configs"


@dataclass
class SourceConfig:
    """Download source specification."""

    method: str = "https"
    job_id: str = ""
    manifest_path: str = ""


@dataclass
class DatasetConfig:
    """Dataset identification."""

    name: str = ""
    schema: str = ""
    symbol: str = "NVDA"
    start_date: str = ""
    end_date: str = ""


@dataclass
class StorageConfig:
    """Output storage specification."""

    output_dir: str = ""


@dataclass
class DownloadParams:
    """Download tuning parameters."""

    parallel: int = 2


@dataclass
class Credentials:
    """Databento credentials loaded from credentials.toml or CLI args."""

    api_key: str = ""


@dataclass
class IngestConfig:
    """Complete download configuration assembled from TOML + CLI overrides."""

    source: SourceConfig = field(default_factory=SourceConfig)
    dataset: DatasetConfig = field(default_factory=DatasetConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    download: DownloadParams = field(default_factory=DownloadParams)
    credentials: Credentials = field(default_factory=Credentials)

    def __post_init__(self):
        if isinstance(self.source, dict):
            self.source = SourceConfig(**self.source)
        if isinstance(self.dataset, dict):
            self.dataset = DatasetConfig(**self.dataset)
        if isinstance(self.storage, dict):
            self.storage = StorageConfig(**self.storage)
        if isinstance(self.download, dict):
            self.download = DownloadParams(**self.download)
        if isinstance(self.credentials, dict):
            self.credentials = Credentials(**self.credentials)


def load_credentials(path: Path | None = None) -> Credentials:
    """Load credentials from TOML file.

    Args:
        path: Path to credentials.toml. Defaults to MODULE_ROOT/credentials.toml.

    Returns:
        Credentials dataclass. Fields may be empty if file doesn't exist.
    """
    cred_path = path or CREDENTIALS_PATH
    if not cred_path.exists():
        return Credentials()

    with open(cred_path, "rb") as f:
        data = tomllib.load(f)

    db_section = data.get("databento", {})
    return Credentials(
        api_key=db_section.get("api_key", ""),
    )


def load_dataset_config(config_path: Path) -> IngestConfig:
    """Load a dataset download config from TOML.

    Args:
        config_path: Path to dataset config file (e.g., configs/datasets/opra_nvda_*.toml)

    Returns:
        IngestConfig with all sections populated

    Raises:
        FileNotFoundError: If config file does not exist
        ValueError: If config fails validation (see validate_config())
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    source = SourceConfig(
        method=data.get("source", {}).get("method", "https"),
        job_id=data.get("source", {}).get("job_id", ""),
        manifest_path=data.get("source", {}).get("manifest_path", ""),
    )
    dataset = DatasetConfig(
        name=data.get("dataset", {}).get("name", ""),
        schema=data.get("dataset", {}).get("schema", ""),
        symbol=data.get("dataset", {}).get("symbol", "NVDA"),
        start_date=data.get("dataset", {}).get("start_date", ""),
        end_date=data.get("dataset", {}).get("end_date", ""),
    )
    storage = StorageConfig(
        output_dir=data.get("storage", {}).get("output_dir", ""),
    )
    download = DownloadParams(
        parallel=data.get("download", {}).get("parallel", 2),
    )

    config = IngestConfig(
        source=source,
        dataset=dataset,
        storage=storage,
        download=download,
    )

    errors = validate_config(config)
    if errors:
        raise ValueError(
            f"Config validation errors in {config_path}: "
            + "; ".join(errors)
        )

    return config


def validate_config(config: IngestConfig) -> list[str]:
    """Validate an IngestConfig for required fields and valid ranges.

    Args:
        config: IngestConfig to validate

    Returns:
        List of error strings (empty if valid)
    """
    errors: list[str] = []

    if config.source.method != "https":
        errors.append(
            f"source.method must be 'https', got '{config.source.method}'"
        )

    if not config.source.job_id and not config.source.manifest_path:
        errors.append(
            "Either source.job_id or source.manifest_path is required"
        )

    if not config.dataset.name:
        errors.append("dataset.name is required")

    if not config.dataset.symbol:
        errors.append("dataset.symbol is required")

    if not config.storage.output_dir:
        errors.append("storage.output_dir is required")

    if config.download.parallel < 1:
        errors.append(
            f"download.parallel must be >= 1, got {config.download.parallel}"
        )
    if config.download.parallel > 8:
        errors.append(
            f"download.parallel should be <= 8 to avoid TCP congestion, "
            f"got {config.download.parallel}"
        )

    return errors
