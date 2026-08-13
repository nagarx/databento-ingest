# databento-ingest

High-throughput, safety-first data acquisition from Databento via HTTPS API.

> **Pipeline scope (2026-06-02).** This module is part of an **intraday trading research pipeline** — an experiment-first platform for discovering and validating *any* profitable **intraday** trading edge (no overnight positions), across approach classes (microstructure/HFT, scalping, intraday momentum, intraday statistical arbitrage, …) and instruments (equities, futures, same-day options). The pipeline *originated* as a high-frequency NVDA MBO/LOB microstructure system — that origin explains the "HFT" / "LOB" / "MBO" naming here — and that microstructure-direction program is now one (largely-closed) track among many. **Names are historical; the mission is general.** This module is one acquisition route: Databento HTTPS download, streaming SHA-256/size verification, and rename-after-verification. The retained SSD corpus also contains objects acquired through other or older routes, so this module is not universal lineage authority. Payload promotion has no data-file/directory `fsync` or same-output writer lock; it is not a power-loss durability guarantee. For the full mission + approach taxonomy + capability-readiness boundary, see root `AGENTS.md`.

> **Custody boundary (2026-08-02).** Read
> [`docs/MANIFESTS_AND_CUSTODY.md`](docs/MANIFESTS_AND_CUSTODY.md) before using,
> moving, merging, re-downloading, or documenting retained Databento data. A
> provider-native receipt, this tool's v1.3 session manifest, a `SHA256SUMS`
> declaration, a condition file, and a fresh audit digest prove different
> things and are not interchangeable.

## Quick Start

### 1. Setup

```bash
cd databento-ingest
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

### 2. Configure credentials

```bash
cp configs/credentials.toml.example credentials.toml
# Edit credentials.toml with your Databento API key
# Get your key: https://databento.com/portal/keys
```

### 3. Download

**Config-driven download (preferred):**

```bash
python -m databento_ingest download \
    --config configs/datasets/opra_nvda_cmbp1_nov2025.toml
```

**Direct download by job ID:**

```bash
python -m databento_ingest download-job \
    --job-id "OPRA-20260305-FP53NRH898" \
    --output-dir "data/OPRA/NVDA/cmbp1_2025-10-29_to_2025-11-24" \
    --symbol NVDA --dataset OPRA
```

**Dry run (list files without downloading):**

```bash
python -m databento_ingest download \
    --config configs/datasets/opra_nvda_cmbp1_nov2025.toml --dry-run
```

### 4. Other commands

```bash
# Submit a new batch job
python -m databento_ingest batch --symbol NVDA --start 2025-11-13 --end 2025-11-25 \
    --dataset OPRA --schema cmbp-1 --output-dir data/OPRA/NVDA/...

# List batch jobs
python -m databento_ingest list-jobs

# Merge datasets between directories
python -m databento_ingest merge --source /path/to/source --target /path/to/target

# Verify downloaded files against manifest checksums
python -m databento_ingest verify --config configs/datasets/opra_nvda_cmbp1_nov2025.toml
```

## Features

- **HTTPS downloads** with Databento API key authentication
- **Verified namespace promotion** — newly transferred files are renamed only after size and SHA-256 match; payload bytes are not `fsync`-durable and concurrent writers are not locked
- **Streaming SHA-256** — verification happens during download, zero extra I/O
- **Resume support** — interrupted downloads resume automatically
- **Parallel downloads** — configurable connections (default: 2 for config-driven, 4 for direct CLI)
- **Disk space checks** — validates free space before starting
- **Session-manifest tracking** — a run that transfers at least one file writes a local v1.3 result record; mixed-run checksums cover newly downloaded files only, and an all-existing run returns without a new manifest

## Architecture

See `CODEBASE.md` for the full technical reference. Before running a **large batch pull**, read `DOWNLOAD_OPERATIONS.md` and `docs/MANIFESTS_AND_CUSTODY.md` — the operational and evidence-role boundaries for resume, single-writer discipline, account-scoped HTTP 403s, independent verification, and retained receipts.
