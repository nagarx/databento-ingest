# databento-ingest -- Technical Reference

High-throughput, safety-first data acquisition from Databento via HTTPS API for the trading research pipeline.

> **Pipeline scope (2026-06-02).** This module is part of an **intraday trading research pipeline** — an experiment-first platform for discovering and validating *any* profitable **intraday** trading edge (no overnight positions), across approach classes (microstructure/HFT, scalping, intraday momentum, intraday statistical arbitrage, …) and instruments (equities, futures, same-day options). The pipeline *originated* as a high-frequency NVDA MBO/LOB microstructure system — that origin explains the "HFT" / "LOB" / "MBO" naming here — and that microstructure-direction program is now one (largely-closed) track among many. **Names are historical; the mission is general.** This module's role: the data-acquisition front door — Databento HTTPS download + streaming SHA-256 verify + atomic writes; acquires any dataset the research needs (equities/futures/options bars or tick/MBO). For the full mission + approach taxonomy + capability-readiness boundary, see root `CLAUDE.md` §Research Scope & Charter (+ `CROSS_ASSET_OFI_FINDINGS_AND_ISSUES_2026_06_01.md` §9).

## Module Structure

```
databento-ingest/
  CODEBASE.md                         # This file
  README.md                           # Quick start
  pyproject.toml                      # Package definition (requires: requests, hft-contracts; optional: databento)
  src/databento_ingest/
    __init__.py                       # Version (0.2.0)
    __main__.py                       # python -m databento_ingest
    cli.py                            # CLI entry point (6 argparse subcommands)
    config.py                         # TOML config loader + validation + 6 dataclass types
    downloader.py                     # HTTPS download engine, Databento manifest loader, DownloadProgress
    manifest.py                       # Our manifest creation/reading/validation (schema v1.3)
    batch.py                          # API batch operations (submit, list-jobs, merge)
  configs/
    credentials.toml.example          # Template (actual file gitignored)
    datasets/
      opra_nvda_cmbp1_nov2025.toml          # OPRA NVDA CMBP-1, Nov 2025 (8 files; now in the merged 19-file cmbp1_2025-10-29_to_2025-11-24 / 595 GB set)
      opra_nvda_statistics_oct_nov2025.toml         # OPRA NVDA statistics (Open Interest), Oct-Nov 2025 (19 files, 33.63 MB; OI companion to the NVDA cmbp1 firehose for GEX/PCP/OOI)
      opra_nvda_definition_oct_nov2025.toml         # OPRA NVDA definition, Oct-Nov 2025 (19 files, 2.85 MB; instrument_id->contract JOIN KEY for the NVDA cmbp1 + statistics sets)
      opra_index_spx_spy_cbbo1m_oct_nov2025.toml  # OPRA INDEX cbbo-1m SPX/SPY/SPXW, Oct-Nov 2025 (19 files, 6.86 GB; index 0DTE/dealer-gamma variance lane)
      opra_index_spx_spy_statistics_oct_nov2025.toml  # OPRA INDEX statistics SPX/SPY/SPXW, Oct-Nov 2025 (19 files, 117 MB; Open-Interest companion to the cbbo-1m set for GEX)
      opra_index_spx_spy_definition_oct_nov2025.toml  # OPRA INDEX definition SPX/SPY/SPXW, Oct-Nov 2025 (19 files, 26 MB; instrument_id->contract JOIN KEY for the cbbo-1m + statistics sets)
      arcx_pillar_nvda_mbo_2025.toml        # ARCX.PILLAR NVDA MBO, Feb 2025 - Jan 2026 (233 files, ~23 GB)
      xnas_basic_nvda_cmbp1_2025.toml       # XNAS.BASIC NVDA CMBP-1, Feb 2025 - Jan 2026 (235 files, ~24 GB)
      xnas_itch_multi10_mbo_2025h2.toml     # XNAS.ITCH 10-stock MBO, Jul 2025 - Jan 2026 (1340 files, ~14 GB; split_symbols=true → per-symbol files)
      glbx_es_nq_ohlcv1s_2025.toml          # GLBX.MDP3 ES+NQ ohlcv-1s (parent symbology ES.FUT,NQ.FUT), Feb 2025 - Jan 2026 (290 files; vol-regime study)
      equs_mini_regime_bbo1s_2025.toml      # EQUS.MINI bbo-1s, 86-name REGIME_UNIVERSE cohort, 2025-01 - 2026-01 (270 files, ~7.3 GB; split_symbols=false; output_dir on external SSD)
      equs_mini_regime_ohlcv1s_2025.toml    # EQUS.MINI ohlcv-1s, 86-name REGIME_UNIVERSE cohort (270 files, ~1.05 GB; split_symbols=false; external-SSD output_dir; DIFFERENT account key than bbo-1s)
      xnas_itch_regime86_imbalance_2025.toml   # NEW (PLAN-021 stage-3): XNAS.ITCH imbalance (NOII / opening-cross) for the 86-name REGIME_UNIVERSE_86 cohort; split_symbols=false → one daily file
      xnas_itch_regime86_status_2025.toml      # NEW: XNAS.ITCH status (halts / SSR) for the 86-name REGIME_UNIVERSE_86 cohort
      xnas_itch_regime86_definition_2025.toml  # NEW: XNAS.ITCH definition (tick size / listing venue / round-lot) for the 86-name REGIME_UNIVERSE_86 cohort
  tests/
    test_config.py                    # Config validation + defaults + credentials (18 tests)
    test_downloader.py                # format_duration, _parse_expected_hash, DownloadProgress (20 tests)
    test_manifest.py                  # Manifest schema + create/read + date extraction + atomic-write + traceability (22 tests)
```

**Test total: 60** (18 + 20 + 22). Hand-typed counts drift with code (hft-rules §11) — run `python -m pytest --collect-only -q` for the live count.

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
                                  manifest.json (our schema v1.3)
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

### Single-file download lifecycle

`download_file()` (downloader.py) is the module's most involved control flow. It runs each file through one bounded retry loop (up to `MAX_RETRIES`); this is the ordered decision-flow that the §Safety Features bullets describe piecewise. Each attempt:

1. **Resume-detect**: if a `<name>.downloading` temp exists and is partial (`0 < size < expected_size`), re-hash it into the running SHA-256 and send an HTTP `Range: bytes=<size>-` header; a temp already at or over the expected size is deleted first. This one top-of-loop check serves BOTH cross-run resume AND cross-retry resume (a preserved partial from a failed attempt re-enters here).
2. **GET**: issue the streamed request (`http_timeout`), then `raise_for_status()`.
3. **206 guard**: if resuming but the response is not HTTP 206 (server ignored `Range`), restart from zero — reset the hasher and offset.
4. **Stream**: for each `CHUNK_SIZE` chunk, append to the temp, update the SHA-256, and feed the rolling `SPEED_WINDOW` speed deque + the aggregate `DownloadProgress`. **Min-speed abort**: if the windowed speed stays below `MIN_SPEED_BPS` for `MIN_SPEED_DURATION`, raise `ConnectionError` (→ retry).
5. **Size check**: `actual_size != expected_size` → delete temp + `ValueError`.
6. **SHA-256 check** against the Databento hash → mismatch → delete temp + `ValueError`.
7. **Atomic promote**: rename the temp to its final path — the file is "final" only at this point (see §Safety Features #1 + DOWNLOAD_OPERATIONS.md §Integrity model) — and mark it done in `DownloadProgress`.
8. **On error**: undo this attempt's bytes in the progress tracker (`reset_file_progress`), then branch on error class — a transient/network error **preserves** the partial temp (so the next attempt resumes) while a `ValueError` (size/hash/malformed-URL) **deletes** it (a known-bad partial). Back off `RETRY_DELAY_BASE * 2**attempt` and retry. After the final attempt the temp is deleted and the file is reported failed.

Returns `(filename, success, error_message, sha256_hex)`; `download_job()` collects these across the thread pool into the session manifest.

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

**Limitation**: `submit_batch_job()` is a minimal single-schema wrapper — it hardcodes those three fields and does NOT set `split_symbols` or `stype_in`. It therefore cannot reproduce the multi-symbol (`split_symbols=false`, `stype_in=raw_symbol`) or `imbalance`/`status`/`definition` cohort jobs that the `xnas_itch_regime86_*` and `equs_mini_regime_*` configs point at — those jobs were submitted externally (their job IDs are recorded in the configs, downloaded via `download`/`download-job`).

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

**Optional fields**: `schema` (omitted when `None`). `checksums` and `metadata` are always present in `create_manifest()` output, but `validate_manifest()` does not require them. Only `checksums` defaults to `{}` (written as `checksums or {}`); `metadata` is a **required** positional argument (no default) and is written verbatim as passed.

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

**Write safety**: Manifest is written atomically via the `hft_contracts.atomic_io.atomic_write_json` SSoT (Class A primitive; tmp + fsync + `os.replace` + BaseException-safe cleanup). Pre-#PY-371 (closed 2026-05-24) used local manual `tmp.rename()` without fsync barrier. Caller pre-validates JSON-serializability via bare `json.dumps(manifest)` before the SSoT call to preserve hft-rules §8 fail-loud-on-non-serializable-types — atomic_write_json's `default=str` silently coerces; see `hft-contracts/src/hft_contracts/atomic_io.py:38-65` §"Canonical convention" for the caller-responsibility note.

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
output_dir = "data/OPRA/NVDA/cmbp1_2025-10-29_to_2025-11-24"  # Required

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

**Two multi-symbol patterns are in use across the configs** (`split_symbols` / `stype_in` are chosen at batch-submission time — externally — not by this tool's config schema; the tool only records the resulting `symbol` field):

- **`split_symbols=true`** (`xnas_itch_multi10_mbo_2025h2.toml`) — a comma-separated `symbol` and per-symbol daily files (`xnas-itch-{DATE}.mbo.{SYMBOL}.dbn.zst`), as above.
- **`split_symbols=false`** (the `xnas_itch_regime86_*` and `equs_mini_regime_*` 86-name pulls) — **one combined daily file per schema** containing all symbols, with `symbol = "REGIME_UNIVERSE_86"` used as a **label only** (it flows to our output manifest's `symbol` field; it is NOT a comma-separated list and NOT a per-file filter). Filenames carry no per-symbol suffix.

## Data Directory Convention

```
data/
  OPRA/
    NVDA/
      cmbp1_2025-10-29_to_2025-11-24/   # {schema}_{start}_to_{end}
        manifest.json                     # Our manifest (schema v1.3)
        opra-pillar-20251113.cmbp-1.dbn.zst
        ...
```

**Convention**:
- Options (OPRA): `data/OPRA/{underlying}/{schema}_{start}_to_{end}/`
- Equities (XNAS): `data/{SYMBOL}_{start}_to_{end}/`
- Regime-universe cohorts (XNAS.ITCH, EQUS.MINI): `data/{DATASET}/REGIME_UNIVERSE/{schema}_{start}_to_{end}/` — one combined file per day (e.g. `data/XNAS_ITCH/REGIME_UNIVERSE/imbalance_2025-02-03_to_2026-01-08/`).
- Futures (GLBX.MDP3): `data/GLBX_MDP3/{scope}/{schema}_{start}_to_{end}/` (e.g. `.../ES_NQ/ohlcv1s_...`).
- `output_dir` may be an absolute path used verbatim — the EQUS.MINI configs point at an external SSD (`/Volumes/WD_Black/HFT-data/...`). (CLI note: a relative `output_dir` resolves against the monorepo root; a relative `manifest_path` resolves against `databento-ingest/` — an asymmetry those configs sidestep with absolute paths.)

## Downstream Boundary / Consumers

databento-ingest is an **entry-point tool** — it exposes no importable Python API to downstream code. Its output boundary is entirely on-disk: it writes `.dbn.zst` data files (+ a v1.3 `manifest.json`) into `output_dir`.

**The `.dbn.zst` files ARE the downstream contract.** Consumers read the zstd-compressed DBN binary directly via Databento's own DBN format libraries — they do NOT read this module's manifest:

- **Rust consumers** parse DBN through Databento's `dbn` crate (pinned to a git tag; see each module's `Cargo.toml`): `MBO-LOB-reconstructor` (the MBO loader, gated behind its default-on `databento` feature — itself composed by `feature-extractor-MBO-LOB` and `mbo-statistical-profiler`, which inherit the loader rather than re-reading files), `basic-quote-processor` (XNAS.BASIC CMBP-1), and `opra-statistical-profiler` (OPRA).
- **Python discovery harnesses** read the `.dbn.zst` via Databento's DBN tooling, but not all through the Python SDK: `nvda_discovery` + `opra_discovery` use the `databento` SDK reader (`db.DBNStore.from_file`), while `glbx_discovery` + `xsec_equity_discovery` decode through the `dbn` CLI binary (`~/.cargo/bin/dbn <file> -J/-C -s`; `glbx_discovery/momentum/loaders.py` notes the `databento` Python package is not installed there, so the CLI is the verified route). Some also ship their own Rust extractors — `glbx_discovery/analysis/extractor_mbp10` (a direct `dbn`-crate dependency) and `xsec_equity_discovery/extractor` (which has no direct `dbn` dep — it pulls the crate transitively via `MBO-LOB-reconstructor`'s default `databento` feature / `DbnLoader`).

**The v1.3 `manifest.json` is a PROVENANCE / record artifact, NOT a consumed contract** — a per-download inventory (file list + verified SHA-256 checksums + traceability metadata for multi-year reproducibility). No downstream module parses its schema (verified: zero sibling references to its distinctive fields such as `download_method` / `ingest_tool_version`). Post-download integrity re-checks use the `verify` subcommand or an independent `SHA256SUMS` (DOWNLOAD_OPERATIONS.md §"Always verify independently"), never the manifest. This module therefore has no code-level output coupling to its siblings beyond the raw DBN file format itself.

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
- `new_bytes`: bytes transferred in the current session only (excludes resumed bytes). Used for BOTH speed AND overall progress.
- `completed_bytes`: total size of fully finished files. Monotonically increasing — **bookkeeping only** (unused/write-only bookkeeping state — never read for display), NOT a progress input.
- Overall progress = `min(new_bytes, total_bytes)`. `completed_bytes` is deliberately NOT added: a completed file's bytes are already accumulated in `new_bytes` (per chunk via `add_chunk`), so `completed_bytes + new_bytes` double-counted them and drove the bar to 100% at ~half-done — the bug removed in commit `0c2b279` and locked by the `TestDownloadProgress` regression tests.

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
| `PARALLEL_DOWNLOADS` | `4` | downloader | Default for `download-job` subcommand; 4 x 35 MB/s saturates ~1.1 Gbps |
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
10. **Atomic manifest write**: Manifest is written via the `hft_contracts.atomic_io.atomic_write_json` SSoT — a unique per-writer tmp file (`<name>.tmp.<pid>.<ns>.<rand4>`) + `fsync` + `os.replace` + BaseException-safe cleanup (see §Manifest Schema → Write safety) — preventing partial or unflushed manifests. (NOT a fixed `manifest.json.tmp` + `rename`; that was the pre-#PY-371 pattern, which lacked the fsync barrier.)
11. **Skip-by-size for existing files**: Files already present with matching byte size are skipped without re-downloading. The `verify` subcommand provides explicit SHA-256 hash verification for previously downloaded files.
12. **Checksums scope**: Checksums recorded in our manifest cover only files downloaded in the current session. Files skipped (already present) are not re-hashed during download; use `verify` for full integrity checks.
13. **Progress accounting on retry**: When a download attempt fails, `DownloadProgress.reset_file_progress()` undoes the byte count from the failed attempt, keeping the aggregate progress bar accurate.
