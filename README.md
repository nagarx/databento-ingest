# databento-ingest

High-throughput, safety-first data acquisition from Databento via HTTPS API.

> **Pipeline scope (2026-06-02).** This module is part of an **intraday trading research pipeline** — an experiment-first platform for discovering and validating *any* profitable **intraday** trading edge (no overnight positions), across approach classes (microstructure/HFT, scalping, intraday momentum, intraday statistical arbitrage, …) and instruments (equities, futures, same-day options). The pipeline *originated* as a high-frequency NVDA MBO/LOB microstructure system — that origin explains the "HFT" / "LOB" / "MBO" naming here — and that microstructure-direction program is now one (largely-closed) track among many. **Names are historical; the mission is general.** This module's role: the data-acquisition front door — Databento HTTPS download + streaming SHA-256 verify + atomic writes; acquires any dataset the research needs (equities/futures/options bars or tick/MBO). For the full mission + approach taxonomy + capability-readiness boundary, see root `CLAUDE.md` §Research Scope & Charter (+ `CROSS_ASSET_OFI_FINDINGS_AND_ISSUES_2026_06_01.md` §9).

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

### 5. Cost-capped batch planning (preferred for new jobs)

Install the approved Databento Python SDK 0.81 line, then create a metadata-only
plan:

```bash
pip install -e '.[api]'
export DATABENTO_API_KEY='db-...'

python -m databento_ingest batch-plan \
    --config configs/requests/xnas_itch_regime86_statistics_20250203_20260710.toml \
    --output xnas-statistics.plan.json
```

`batch-plan` calls only Databento metadata endpoints. The plan records the exact
request fingerprint, SDK version, top-level dataset range, requested-schema
range, available schemas, returned per-date dataset-condition rows, estimated
cost, record count, billable bytes, timestamp, and its own canonical SHA-256.
It is created atomically and will not overwrite an existing path. A missing,
malformed, or narrower requested-schema range fails closed. Empty condition
metadata, duplicate dates, and out-of-request dates are invalid. Any returned
`pending` or `missing` condition blocks the plan; `degraded` remains visible but
does not alone block submission. Provider condition responses are not assumed
to contain one row per calendar or trading day: the plan explicitly records
`condition_coverage = "provider_rows_unverified"`. Detecting an omitted trading
session is deferred to a provenance-bearing venue-calendar/landing coverage
gate. The availability decision is rederived from the hashed query and metadata
when a plan is created, loaded, and submitted; stored booleans are not trusted.
TOML sections and fields and every nested plan object use exact allowlists;
unknown material, including secret-like fields, is rejected. Provider range
timestamps retain their full ISO timestamp: date-only values mean UTC midnight,
timezone-aware values are normalized to UTC for coverage comparisons, and
timezone-naive timestamps are rejected.

Review the plan, then submit it within 15 minutes:

```bash
python -m databento_ingest batch-submit \
    --config configs/requests/xnas_itch_regime86_statistics_20250203_20260710.toml \
    --plan xnas-statistics.plan.json
```

`batch-submit` verifies the plan hash and request fingerprint, rejects stale,
unavailable, or over-cap plans, and immediately re-quotes the same provider
query. It rechecks the 15-minute plan age after that quote, durably records the
attempt, samples the clock again immediately before the provider call, and
submits only if every guard still passes and the fresh quote remains within the
configured cap and immutable absolute ceiling of USD 1.00. A final freshness
failure is durably marked `aborted` and makes no batch submission. The command
writes `xnas-statistics.plan.receipt.json` with the job ID, an explicit safe
allowlist projection of the provider response, estimates, fresh quote, SDK
version, timestamps, submission identity, and receipt SHA-256.
Neither command accepts an API key argument; credentials come from
`DATABENTO_API_KEY` or the ignored `credentials.toml`, and are never serialized.

Before the fresh quote, submission reserves a SHA-256 identity derived only
from the exact `batch.submit_job` argument tuple. A durable private ledger at
`~/.local/state/databento-ingest/submissions/` uses no-follow directory file
descriptors and exact expected-state transitions. Existing roots are never
chmodded: the root and `submissions` must be owned by the effective user at mode
`0700`; identity locks and journals must be singly linked regular files at mode
`0600`. A descriptor-anchored POSIX lock is held from reservation through the
terminal transition. State is an append-only canonical JSON-lines journal with
sequence numbers and a SHA-256 predecessor chain: transitions never replace or
unlink the journal pathname. Symlinks, corruption, concurrent cooperative
writers, or a changed lock, record, or root binding fail closed. The normal
states are `reserved -> attempted -> consumed`; post-attempt local refusals use
`aborted`. A proven pre-attempt refusal appends `released`; a later retry under
the same identity appends a new `reserved` attempt. Any ambiguity after
`attempted` remains blocked for manual provider-account reconciliation.

The journal has an exact 1 MiB byte ceiling. Every append checks projected
`current bytes + canonical UTF-8 line bytes` before writing. Before a
`reserved` line becomes durable, the ledger reserves enough remaining capacity
for either `released` or `attempted` plus the larger of `aborted` and
`consumed`; it repeats that headroom check before `attempted`. A capacity
refusal leaves an existing journal byte-for-byte unchanged and happens before
client construction. There is no automatic compaction or capacity override:
preserve the journal, do not switch or delete the state root, and reconcile
that exact identity manually before any further submission. A valid final
`reserved` left by a dead writer is different: after the exclusive lock is
acquired, it is provably pre-provider under this protocol, so it is appended as
`released` with reason `recovered_orphaned_reservation` and retried. Partial or
corrupt journals remain blocked.

This is local at-most-once protection only while one selected state root remains
intact; it is not provider exactly-once delivery. `DATABENTO_INGEST_STATE_DIR`
must be absolute and selects a separate local idempotency domain intended for
isolated tests. Switching, deleting, or replacing that root invalidates the
cross-run guarantee. Within one intact root, a copied, renamed, or freshly
generated equivalent plan remains blocked by the same submission identity.
The lock coordinates writers that honor this ledger protocol; it is not a
security boundary against a same-user process deliberately bypassing it.

Request `end` is exclusive. Databento's dataset-condition endpoint uses an
inclusive `end_date`, so the planner queries conditions through `end - 1 day`.
For example, coverage through 2026-07-10 is represented by
`end = "2026-07-11"`.

Requests are bounded to 2,000 unique symbols and each symbol is bounded to 128
UTF-8 bytes. Provider job IDs are bounded to 256 UTF-8 bytes before they can be
written to a terminal journal record. Submission freshness samples the UTC
wall clock independently at reservation, attempted-state construction,
immediately before provider submission, and consumption. Tests may inject an
advancing callable clock; a scalar timestamp is rejected so it cannot freeze a
production submission's freshness checks.

The legacy `batch` command remains available for compatibility but submits
without the plan/fingerprint/cost-cap safety gates and is deprecated for new
acquisitions.

## Features

- **HTTPS downloads** with Databento API key authentication
- **Atomic writes** — files are only finalized after SHA-256 verification
- **Streaming SHA-256** — verification happens during download, zero extra I/O
- **Resume support** — interrupted downloads resume automatically
- **Parallel downloads** — configurable connections (default: 2 for config-driven, 4 for direct CLI)
- **Disk space checks** — validates free space before starting
- **Manifest tracking** — every download produces a manifest.json
- **Cost-capped planning** — typed requests, metadata-only estimates, and a mandatory fresh re-quote
- **No-clobber provenance** — canonical plan/receipt JSON plus a durable identity ledger

## Architecture

See `CODEBASE.md` for the full technical reference. Before running a **large batch pull**, read `DOWNLOAD_OPERATIONS.md` — the operational playbook (resume semantics, the single-process rule, account-scoped HTTP 403s, independent `SHA256SUMS` verification, the chunked/time-boxed wrapper).
