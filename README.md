# databento-ingest

High-throughput, safety-first data acquisition from Databento via HTTPS API.

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
    --output-dir "data/OPRA/NVDA/cmbp1_2025-11-13_to_2025-11-25" \
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
- **Atomic writes** — files are only finalized after SHA-256 verification
- **Streaming SHA-256** — verification happens during download, zero extra I/O
- **Resume support** — interrupted downloads resume automatically
- **Parallel downloads** — configurable connections (default: 2 for config-driven, 4 for direct CLI)
- **Disk space checks** — validates free space before starting
- **Manifest tracking** — every download produces a manifest.json

## Architecture

See `CODEBASE.md` for the full technical reference.
