# Download Operations — large resumable Databento batch downloads

> Operational playbook + known issues, distilled from acquiring the GLBX (24 GB) and
> OPRA (296 GB downloaded this campaign; the merged NVDA OPRA set is now 595 GB) datasets in 2026-05/06 over a slow, bufferbloat-prone link.
> Pairs with `CODEBASE.md` (the tool internals). Last updated 2026-06-14.

## The method (what worked)
- **`download-job` is the entry point** for a pre-submitted Databento batch job:
  `python -m databento_ingest download-job --job-id <ID> --output-dir <dir> --manifest <local manifest.json> --symbol <S> --dataset <DS> --parallel 1 --api-key <key>`.
- **Single-stream (`--parallel 1`)** is optimal on a bandwidth-capped link (see §Connection). Parallel only fragments into more partial temps with no throughput gain.
- **Resumable by design**: completed files skip by exact size; partial `<name>.downloading` temps Range-resume. Re-run the *same* command to resume. **Never run two `download-job` processes on the same dir** (no lockfile).
- **Integrity model (trust it):** a temp is promoted to its final name ONLY after both byte-size AND full SHA-256 (vs the manifest) pass (`downloader.py` ~:487-502). So **no interruption — SIGTERM, SIGKILL, power loss — can leave a corrupt *final* file**; the worst case is a re-hashable partial temp.
- **Always verify independently after**: each dataset dir gets a `SHA256SUMS`; re-check with `cd <dir> && shasum -a 256 -c SHA256SUMS`. Do not trust the tool's exit code alone.

## The chunked / time-boxed wrapper
`data/logs/opra_chunk_runner.py` (on the SSD; not under git) wraps `download-job` to:
- run for a wall-clock budget (`--max-seconds`), then SIGTERM → grace → SIGKILL (resumable);
- **source `--api-key` from the `DATABENTO_API_KEY` env var (fail-loud)** — never hardcode the key;
- guard against a concurrent run; log per-chunk; print a progress report at the end;
- run under `caffeinate -ims` (required — see §Connection).
- **⚠️ It is currently hardcoded to one job (`JOB_ID`/`MANIFEST`/`OUTPUT_DIR` constants). Parameterize these (or pass as args) before reusing for a new job.**
- Launch (run-to-completion w/ 36 h backstop): `DATABENTO_API_KEY=<key> caffeinate -ims <venv>/python data/logs/opra_chunk_runner.py --logfile <path> --max-seconds 129600`. For 12 h chunks use `--max-seconds 43200` and re-run with a fresh `--logfile` to resume.

## Connection nature (this machine/link, 2026-06)
- **Hard ~2.7 MB/s download cap** (per-link, not per-connection) — measured at parallel=1/2/4 → 2.71/2.30/2.69 MB/s aggregate. **Bandwidth-limited, ~20 Mbps.**
- **Bufferbloat**: `networkQuality` responsiveness LOW (~900-970 ms under load), but **stable** (0% packet loss; ~1 connection break per ~20 h, cleanly resumed).
- **Keep VPN OFF for Databento**: VPN-on gave ~0.5 MB/s with *frequent* `ChunkedEncodingError` drops; VPN-off gave ~2.7 MB/s and near-zero drops.
- Practical: a ~300 GB CMBP-1 download takes ~24-36 h single-stream. Use `caffeinate -ims` (the Mac's `disksleep` would otherwise spin down the external SSD / idle-sleep mid-run).

## Known tool issues / follow-ups (non-blocking)
1. **`[Overall]` progress/ETA resets after a Range-resume** → prints `0.0 MB/s` and absurd ETA (e.g. "9327h"). Cosmetic — the per-file % and on-disk bytes stay correct. The overall-bytes aggregator should account for the resume offset.
2. **No `fsync` before `rename`** on the multi-GB data files (`downloader.py` ~:502) — a power-loss between rename and the OS flushing dirty pages could in theory leave a final-named file with an unflushed tail (skipped by size on re-run). **Mitigated** by the mandatory independent SHA-256 re-verify. Worth a proper fix (fsync the temp fd before rename). (Note: this caveat is about the *data files*; the *manifest* IS fsync-atomic via `atomic_write_json`.)
3. **No env-var API-key path** — only `--api-key` or `credentials.toml`. (The wrapper bridges `DATABENTO_API_KEY` → `--api-key`.)
4. **Download URLs are account-scoped** (`…/batch/download/<ACCOUNT>/<job>/…`): the key must belong to the job's account, else **HTTP 403 `auth_user_does_not_match_api_key`**. Multi-account users: match key to job.
5. **No `gtimeout`/`timeout` on this macOS** — the Python wrapper provides the budget instead.
6. **Sidecar manifest**: on completion `download-job` writes its own v1.3 `manifest.json` listing only the *session's* files — it does **not** describe pre-existing files in a merge dir. For merged dirs, rely on `SHA256SUMS` + `DATASET.md`.
