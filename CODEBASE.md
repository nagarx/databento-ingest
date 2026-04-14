# databento-ingest -- Technical Reference

High-throughput, safety-first data acquisition from Databento via HTTPS API for the HFT pipeline.

## Module Structure

```
databento-ingest/
  CODEBASE.md                         # This file
  README.md                           # Quick start
  pyproject.toml                      # Package definition (requires: requests; optional: databento)
  src/databento_ingest/
    __init__.py                       # Version (0.2.0)
    __main__.py                       # python -m databento_ingest
    cli.py                            # CLI entry point (6 argparse subcommands)
    config.py                         # TOML config loader + validation + 6 dataclass types
    downloader.py                     # HTTPS download engine, Databento manifest loader, DownloadProgress
    manifest.py                       # Our manifest creation/reading/validation (schema v1.2)
    batch.py                          # API batch operations (submit, list-jobs, merge)
  configs/
    credentials.toml.example          # Template (actual file gitignored)
    datasets/
      opra_nvda_cmbp1_nov2025.toml          # OPRA NVDA CMBP-1, Nov 2025 (8 files, ~278 GB)
      arcx_pillar_nvda_mbo_2025.toml        # ARCX.PILLAR NVDA MBO, Feb 2025 - Jan 2026 (233 files, ~23 GB)
      xnas_basic_nvda_cmbp1_2025.toml       # XNAS.BASIC NVDA CMBP-1, Feb 2025 - Jan 2026 (235 files, ~24 GB)
      xnas_itch_multi10_mbo_2025h2.toml     # XNAS.ITCH 10-stock MBO, Jul 2025 - Jan 2026 (1340 files, ~14 GB)
  tests/
    test_config.py                    # Config validation + defaults + credentials (18 tests)
    test_downloader.py                # format_duration, _parse_expected_hash (17 tests)
    test_manifest.py                  # Manifest schema + create/read + date extraction + Databento manifest loading (19 tests)
```

**Test total: 54** (18 + 17 + 19).

## Data Flow

```
Dataset TOML config ──> cli.py ──> config.py (load + validate)
                                       |
credentials.toml ─────> cli.py ──> api_key
                                       |
                                       v
                                  downloader.py
                                       |
                        +--------------+---------------+
                        v              v               v
                   Thread 1       Thread 2         Thread N
                   (per-thread requests.Session)
                   GET https://hist.databento.com/...
                   Auth: api_key as Basic Auth username
                        |              |               |
                        v              v               v
                   .downloading   .downloading    .downloading
                   + streaming SHA-256 verification
                        |              |               |
                        v              v               v
                   SHA-256 match? ──> atomic rename to final
                   SHA-256 fail?  ──> delete + hard error
                                       |
                                       v
                                  manifest.py
                                       |
                                       v
                                  manifest.json (our schema v1.2)
```

## Download Protocol

### Why HTTPS (not FTP)

FTP caused TCP congestion collapse on the transatlantic path (107ms RTT to Databento's
Boston server), producing boom-bust speed oscillations (35 MB/s -> 0.3 MB/s). Root cause:
FTP's split control/data channels combined with aggressive `SO_RCVBUF` settings triggered
repeated TCP slow-start recoveries.

HTTPS delivers **35 MB/s stable** with zero custom tuning. Databento's own Rust SDK
(`databento-rs`) uses HTTPS exclusively for batch downloads -- FTP is a secondary option
on their side.

### Authentication

Databento API key as HTTP Basic Auth username with empty password, matching the convention
used by Databento's API and official SDKs.

### File Discovery

Two modes for obtaining the file manifest:

1. **API mode** (default): calls `GET /v0/batch.list_files?job_id=...` on `hist.databento.com`
2. **Manifest mode**: loads a local Databento `manifest.json` (which contains HTTPS URLs and SHA-256 hashes)

Both produce the same normalized file list: `[{filename, size, hash, https_url}, ...]`. Only `.dbn.zst` data files are extracted; metadata files (`condition.json`, `metadata.json`) are filtered out.

## CLI Subcommands

Entry point: `python -m databento_ingest <subcommand>` or `databento-ingest <subcommand>` (via `project.scripts`).

### download

Config-driven download from a TOML dataset specification.

| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--config` | Yes | -- | Path to dataset config TOML (relative to `databento-ingest/` or absolute) |
| `--dry-run` | No | false | List files without downloading |
| `--api-key` | No | `""` | Override credentials.toml API key |
| `--connect-timeout` | No | 30 | HTTP connect timeout in seconds |
| `--read-timeout` | No | 120 | HTTP read timeout in seconds (per chunk) |
| `--estimated-speed` | No | 3.0 | Estimated speed in MB/s for time estimates |

Parallelism is controlled by the `[download].parallel` field in the config TOML (default: 2).

### download-job

Direct download by job ID without a TOML config file.

| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--job-id` | Yes | -- | Databento batch job ID |
| `--output-dir` | Yes | -- | Output directory |
| `--symbol` | No | `"NVDA"` | Symbol name for manifest metadata |
| `--dataset` | No | `"XNAS.ITCH"` | Databento dataset identifier |
| `--parallel` | No | 4 | Parallel download connections (uses `PARALLEL_DOWNLOADS` constant) |
| `--manifest` | No | `""` | Path to local Databento manifest.json (skips API call) |
| `--dry-run` | No | false | List files without downloading |
| `--api-key` | No | `""` | Override credentials.toml API key |

Note: `download-job` defaults to `PARALLEL_DOWNLOADS=4`, while `download` uses the config file's `[download].parallel` (default: 2). The `download-job` subcommand does NOT accept `--connect-timeout`, `--read-timeout`, or `--estimated-speed`.

### verify

Verify downloaded files against a Databento manifest's SHA-256 checksums. Recomputes the hash for each file on disk and reports per-file pass/fail. Exits with code 1 if any file fails.

| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--config` | Yes | -- | Path to dataset config TOML (uses `source.manifest_path` from the config) |

Requires `source.manifest_path` in the config to point to a Databento manifest.

### batch

Submit a new batch request via the Databento API. **Requires**: `pip install databento` (optional dependency).

| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--api-key` | No | `""` | Databento API key |
| `--dataset` | No | `"XNAS.ITCH"` | Dataset identifier |
| `--symbol` | Yes | -- | Symbol to download |
| `--start` | Yes | -- | Start date (YYYY-MM-DD) |
| `--end` | Yes | -- | End date (YYYY-MM-DD) |
| `--output-dir` | Yes | -- | Output directory |
| `--schema` | No | `"mbo"` | Data schema |

Submits with `split_duration="day"`, `encoding="dbn"`, `compression="zstd"`.

### list-jobs

List batch jobs from the Databento API. **Requires**: `pip install databento`.

| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--api-key` | No | `""` | Databento API key |
| `--status` | No | None | Filter by status: `pending`, `processing`, `done`, `expired` |

### merge

Merge `.dbn.zst` files from a source directory into a target directory.

| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--source` | Yes | -- | Source directory |
| `--target` | Yes | -- | Target directory |
| `--dry-run` | No | false | Show what would be done |

Skips files that already exist with matching size. Overwrites on size mismatch.

## Manifest Schema (v1.3)

Every downloaded dataset produces a `manifest.json` (our format, distinct from Databento's):

```json
{
  "schema_version": "1.3",
  "symbol": "NVDA",
  "dataset": "OPRA",
  "schema": "cmbp-1",
  "source": "databento",
  "download_method": "https",
  "date_range": "2025-11-13 to 2025-11-25",
  "download_timestamp": "2026-03-06T12:00:00+00:00",
  "file_count": 8,
  "files": ["opra-pillar-20251113.cmbp-1.dbn.zst", "..."],
  "checksums": {
    "opra-pillar-20251113.cmbp-1.dbn.zst": "sha256_hex..."
  },
  "metadata": {
    "ingest_tool_version": "0.2.0",
    "databento_api_version": 0,
    "job_id": "OPRA-20260305-FP53NRH898",
    "total_size_bytes": 298700000000,
    "failed_files": [],
    "download_speed_mbps": 35.1,
    "download_elapsed_seconds": 8520.3,
    "parallel_connections": 2
  }
}
```

**Required fields**: `schema_version`, `symbol`, `dataset`, `source`, `download_method`, `date_range`, `download_timestamp`, `file_count`, `files`.

**Optional fields**: `schema` (omitted when `None`). `checksums` and `metadata` are always written by `create_manifest()` (default to `{}`) but `validate_manifest()` does not require them to be present.

**Metadata fields written by the downloader** (all present when `download_job()` creates the manifest):
- `ingest_tool_version`: Version of the databento-ingest tool (from `__version__`). Enables multi-year reproducibility.
- `databento_api_version`: Databento API version (from `DATABENTO_API_VERSION`). Detects upstream API schema changes.
- `job_id`: Databento batch job ID.
- `total_size_bytes`: Sum of expected file sizes (int).
- `failed_files`: List of filenames that failed to download (empty list on success).
- `download_speed_mbps`: Average speed over newly-downloaded bytes (float, 2 decimals).
- `download_elapsed_seconds`: Wall-clock elapsed time for the parallel download phase (float, 1 decimal).
- `parallel_connections`: Number of parallel connections used (int).

**Invariant**: `file_count == len(files)`.

**Schema evolution**:
- **v1.2 → v1.3** (additive, non-breaking): added `metadata.ingest_tool_version` (from `__version__`) and `metadata.databento_api_version` (from `DATABENTO_API_VERSION`) for multi-year reproducibility. Old v1.2 manifests on disk remain readable; downstream consumers see the new fields as additional `metadata` dict entries.

**Write safety**: Manifest is written atomically via write-to-`.tmp`-then-rename.

## Dataset Config Schema

Each dataset download is specified in a TOML file under `configs/datasets/`:

```toml
[source]
method = "https"                                          # Required: only "https"
job_id = "OPRA-20260305-FP53NRH898"                       # Databento batch job ID
manifest_path = "jobs/OPRA-20260305-FP53NRH898/manifest.json"  # Optional: skip API call

[dataset]
name = "OPRA"                     # Required: Databento dataset identifier
schema = "cmbp-1"                 # Optional: data schema
symbol = "NVDA"                   # Required: trading symbol (comma-separated for multi-symbol)
start_date = "2025-11-13"        # Optional: for documentation/metadata
end_date = "2025-11-25"          # Optional: for documentation/metadata

[storage]
output_dir = "data/OPRA/NVDA/cmbp1_2025-11-13_to_2025-11-25"  # Required

[download]
parallel = 2                      # Optional: parallel connections (default: 2, max: 8)
```

**Validation rules** (fail-fast per hft-rules.md):
- `source.method` must be `"https"`
- Either `source.job_id` or `source.manifest_path` required
- `dataset.name` required
- `dataset.symbol` required
- `storage.output_dir` required
- `download.parallel` must be 1-8

**Note on `manifest_path`**: Paths under `jobs/` are gitignored. The `jobs/` directory stores Databento-provided manifest bundles that are too large or sensitive to commit.

**Note on `symbol`**: Accepts comma-separated symbols for multi-symbol batch jobs (e.g., `"CRSP,DKNG,FANG,..."`). See Multi-Symbol Support below.

## Credentials

Credentials are loaded from `credentials.toml` (gitignored), with CLI `--api-key` override:

```toml
[databento]
api_key = "db-YOUR_API_KEY"
```

**Security**: Never committed. Copy `credentials.toml.example` to `credentials.toml` and fill in.

**Resolution order**: CLI `--api-key` overrides `credentials.toml`. If neither provides a key, the CLI exits with an error message pointing to `https://databento.com/portal/keys`.

## Multi-Symbol Support

The `dataset.symbol` field accepts a comma-separated string for multi-symbol batch jobs:

```toml
symbol = "CRSP,DKNG,FANG,HOOD,IBKR,ISRG,MRNA,PEP,SNAP,ZM"
```

When Databento processes a multi-symbol job with `split_symbols=true`, it produces per-symbol daily files with the naming pattern:

```
xnas-itch-{DATE}.mbo.{SYMBOL}.dbn.zst
```

The downloader passes the symbol string through to manifest metadata without parsing. All files for all symbols are downloaded into the same `output_dir`. The `extract_date_range()` function handles per-symbol filenames by extracting the 8-digit date from any position in the hyphen-delimited filename.

## Data Directory Convention

```
data/
  OPRA/
    NVDA/
      cmbp1_2025-11-13_to_2025-11-25/   # {schema}_{start}_to_{end}
        manifest.json                     # Our manifest (schema v1.2)
        opra-pillar-20251113.cmbp-1.dbn.zst
        ...
```

**Convention**:
- Options (OPRA): `data/OPRA/{underlying}/{schema}_{start}_to_{end}/`
- Equities (XNAS): `data/{SYMBOL}_{start}_to_{end}/`

## Types Reference

### config.py -- 6 Dataclass Types

| Type | Fields | Defaults | Purpose |
|------|--------|----------|---------|
| `SourceConfig` | `method: str`, `job_id: str`, `manifest_path: str` | `"https"`, `""`, `""` | Download source specification |
| `DatasetConfig` | `name: str`, `schema: str`, `symbol: str`, `start_date: str`, `end_date: str` | `""`, `""`, `"NVDA"`, `""`, `""` | Dataset identification |
| `StorageConfig` | `output_dir: str` | `""` | Output storage path |
| `DownloadParams` | `parallel: int` | `2` | Download tuning |
| `Credentials` | `api_key: str` | `""` | Databento API key |
| `IngestConfig` | `source`, `dataset`, `storage`, `download`, `credentials` | Factory defaults | Complete config assembled from TOML + CLI |

`IngestConfig.__post_init__` converts raw dict sections to their typed dataclass equivalents.

### downloader.py -- DownloadProgress

Thread-safe aggregate progress tracker for multi-file parallel downloads. Uses `threading.Lock` for all state mutations.

**Accounting model**:
- `new_bytes`: bytes transferred in the current session only (excludes resumed bytes). Used for speed calculation.
- `completed_bytes`: total size of fully finished files. Monotonically increasing.
- Overall progress = `min(completed_bytes + new_bytes, total_bytes)`.

**Constructor**: `DownloadProgress(total_files: int, total_bytes: int)`

| Method | Signature | Description |
|--------|-----------|-------------|
| `add_chunk` | `(n_bytes: int) -> None` | Record newly downloaded bytes (called per chunk from download threads) |
| `file_done` | `(file_bytes: int) -> None` | Record a fully completed file (called once per file) |
| `reset_file_progress` | `(bytes_to_subtract: int) -> None` | Undo `add_chunk` calls from a failed attempt before retry |
| `summary_line` | `() -> str` | Format overall progress: files, GB, percent, speed, ETA |

**Speed calculation**: `speed = new_bytes / elapsed`. Uses only new bytes (not resumed) to give accurate current-session throughput. ETA: `remaining / speed`.

## Constants

| Constant | Value | Module | Rationale |
|----------|-------|--------|-----------|
| `DATABENTO_API_BASE` | `"https://hist.databento.com"` | downloader | Databento Historical API gateway |
| `DATABENTO_API_VERSION` | `0` | downloader | API version for URL construction (`/v0/batch.list_files`) |
| `CHUNK_SIZE` | 4 MB (`4 * 1024 * 1024`) | downloader | Per `iter_content()` call; ~24 calls per 96 MB file vs ~96 at 1 MB |
| `MAX_RETRIES` | `5` | downloader | With exponential backoff: 10, 20, 40, 80, 160s delays |
| `RETRY_DELAY_BASE` | `10` seconds | downloader | Base for exponential backoff: `10 * (2 ** attempt)` |
| `PARALLEL_DOWNLOADS` | `4` | downloader | Default for `download-job` subcommand; 4 x 35 MB/s saturates ~430 Mbps |
| `PROGRESS_INTERVAL` | `5` seconds | downloader | Minimum interval between per-file progress log lines |
| `SPEED_WINDOW` | `30` seconds | downloader | Rolling window for speed average (deque of (time, bytes) samples) |
| `DISK_SPACE_SAFETY_MARGIN` | `0.95` | downloader | 5% headroom; need `remaining_size < free * 0.95` |
| `DEFAULT_HTTP_TIMEOUT` | `(30, 120)` | downloader | (connect, read) in seconds; read is per-chunk, not per-transfer |
| `MIN_SPEED_BPS` | `50,000` (50 KB/s) | downloader | Abort threshold -- well below any usable connection |
| `MIN_SPEED_DURATION` | `60` seconds | downloader | Duration below `MIN_SPEED_BPS` before triggering abort + retry |
| `DEFAULT_ESTIMATED_SPEED_MBS` | `3.0` MB/s | downloader | Conservative default for dry-run time estimates |
| `MANIFEST_SCHEMA_VERSION` | `"1.3"` | manifest | Our manifest format version (bumped from 1.2 when traceability metadata fields were added) |
| `MANIFEST_FILENAME` | `"manifest.json"` | manifest | Output manifest filename |
| `DownloadParams.parallel` | `2` (default) | config | Default parallelism for config-driven `download` subcommand |

## Safety Features

1. **Atomic writes**: Downloads write to `.downloading` temp file, rename to final path only after BOTH size AND SHA-256 verification pass.
2. **SHA-256 verification against Databento hashes**: Hash computed during download at zero extra I/O cost (streaming `hashlib.sha256().update()` per chunk), then compared against Databento's authoritative hash. **Mismatch is a hard error** -- file is deleted, not accepted.
3. **Disk space pre-check**: Validates sufficient space for *remaining* (not yet downloaded) files before starting, with 5% safety margin.
4. **Resume support**: Already-completed files (verified by exact byte size) are skipped. Partial `.downloading` files are preserved across runs and resumed via HTTP Range headers. HTTP 206 response is verified -- if the server ignores the Range header, the download restarts cleanly.
5. **Retry with exponential backoff**: Up to 5 attempts per file for transient network errors. Delays: 10, 20, 40, 80, 160 seconds.
6. **Smart stale temp cleanup**: Removes `.downloading` files only for files NOT in the current download queue. Partial temps for files about to be downloaded are preserved for resume.
7. **Per-thread sessions**: Each download thread creates its own `requests.Session` to avoid thread-safety issues with shared session state. Each session sets `User-Agent: databento-ingest/{version}`.
8. **Manifest tracking**: Every download produces a manifest.json with file inventory and verified checksums.
9. **Min-speed enforcement**: If rolling speed drops below 50 KB/s for 60 continuous seconds, the download is aborted with a `ConnectionError` and retried. Prevents indefinite crawling on degraded connections while avoiding false triggers on brief dips.
10. **Atomic manifest write**: Manifest is written to `manifest.json.tmp` then renamed to `manifest.json`, preventing partial writes from corrupting the manifest.
11. **Skip-by-size for existing files**: Files already present with matching byte size are skipped without re-downloading. The `verify` subcommand provides explicit SHA-256 hash verification for previously downloaded files.
12. **Checksums scope**: Checksums recorded in our manifest cover only files downloaded in the current session. Files skipped (already present) are not re-hashed during download; use `verify` for full integrity checks.
13. **Progress accounting on retry**: When a download attempt fails, `DownloadProgress.reset_file_progress()` undoes the byte count from the failed attempt, keeping the aggregate progress bar accurate.
