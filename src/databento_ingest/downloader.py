"""HTTPS downloader for Databento batch data with atomic writes and integrity checks.

Core download engine for Databento data acquisition. Uses the Databento HTTPS API
with API key authentication. Handles parallel downloads, streaming SHA-256
verification against Databento's authoritative checksums, resume via HTTP Range
headers, and atomic writes.

Protocol choice rationale:
    FTP caused TCP congestion collapse on transatlantic paths (107ms RTT to Boston),
    producing boom-bust speed oscillations (35 MB/s → 0.3 MB/s). HTTPS delivers
    35 MB/s stable with zero custom tuning. Databento's own Rust SDK (databento-rs)
    uses HTTPS exclusively for batch downloads.

Authentication:
    Databento API key as HTTP Basic Auth username (empty password).
    Matches the convention used by Databento's API and official SDKs.

Author: HFT Pipeline
"""

import hashlib
import json
import shutil
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import requests

from databento_ingest import __version__
from databento_ingest.manifest import create_manifest, extract_date_range

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATABENTO_API_BASE = "https://hist.databento.com"
DATABENTO_API_VERSION = 0

# 4 MB per chunk for response.iter_content(). Reduces Python call overhead
# (~24 calls per 96 MB file vs ~96 at 1 MB). The OS TCP stack handles
# buffering underneath; larger chunks amortize per-call cost on fast links.
CHUNK_SIZE = 4 * 1024 * 1024

MAX_RETRIES = 5
RETRY_DELAY_BASE = 10  # seconds; exponential backoff: 10, 20, 40, 80, 160

# Default parallel connections. Optimal depends on bandwidth:
#   Fast link (~35 MB/s+): 4 connections compensate for per-connection RTT
#   Slow link (~1-5 MB/s): 2 connections avoid per-connection overhead
PARALLEL_DOWNLOADS = 4

PROGRESS_INTERVAL = 5  # seconds between per-file progress lines
SPEED_WINDOW = 30  # seconds for rolling speed average

DISK_SPACE_SAFETY_MARGIN = 0.95

# Timeout for HTTP requests: (connect_timeout, read_timeout) in seconds.
# read_timeout is per-chunk, not per-transfer. Configurable via CLI.
DEFAULT_HTTP_TIMEOUT = (30, 120)

# Minimum speed enforcement: abort + retry if speed falls below this
# for the specified duration. Prevents crawling indefinitely on degraded
# connections while avoiding false triggers on brief dips.
MIN_SPEED_BPS = 50_000    # 50 KB/s — well below any usable connection
MIN_SPEED_DURATION = 60   # seconds below min speed before triggering retry

# Default estimated speed for time estimates (conservative for slow links)
DEFAULT_ESTIMATED_SPEED_MBS = 3.0  # MB/s

_print_lock = Lock()


def safe_print(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs, flush=True)


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration string.

    Args:
        seconds: Duration in seconds (must be finite and non-negative)

    Returns:
        Formatted string like '2h 15m', '45m 30s', or '12s'
    """
    if seconds < 0 or not (seconds == seconds):  # NaN check without numpy
        return "???"
    seconds = int(seconds)
    if seconds >= 3600:
        h, remainder = divmod(seconds, 3600)
        m = remainder // 60
        return f"{h}h {m:02d}m"
    if seconds >= 60:
        m, s = divmod(seconds, 60)
        return f"{m}m {s:02d}s"
    return f"{seconds}s"


# ---------------------------------------------------------------------------
# Aggregate progress tracker (thread-safe)
# ---------------------------------------------------------------------------


class DownloadProgress:
    """Thread-safe aggregate progress for multi-file downloads.

    Tracks bytes downloaded across all threads and provides an overall
    summary line with file count, total GB, speed, and ETA.

    Accounting model:
        - new_bytes: bytes transferred in the current session (excludes resume)
        - completed_bytes: total size of fully completed files
        - Both are monotonically increasing. Speed uses new_bytes only.
        - Overall progress uses completed_bytes + new_bytes (capped at total).
    """

    def __init__(self, total_files: int, total_bytes: int):
        self._lock = Lock()
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.completed_files = 0
        self.completed_bytes = 0
        self.new_bytes = 0  # only bytes transferred THIS session (for speed)
        self.start_time = time.time()

    def add_chunk(self, n_bytes: int) -> None:
        """Record newly downloaded bytes (NOT resume bytes)."""
        with self._lock:
            self.new_bytes += n_bytes

    def file_done(self, file_bytes: int) -> None:
        """Record a fully completed file. Called once per file."""
        with self._lock:
            self.completed_files += 1
            self.completed_bytes += file_bytes

    def reset_file_progress(self, bytes_to_subtract: int) -> None:
        """Reset progress for a file that failed and will be retried.

        Called before retry to undo add_chunk() calls from the failed attempt.
        """
        with self._lock:
            self.new_bytes = max(self.new_bytes - bytes_to_subtract, 0)

    def summary_line(self) -> str:
        with self._lock:
            elapsed = time.time() - self.start_time
            # Speed: only new bytes transferred this session
            speed = self.new_bytes / max(elapsed, 0.1)
            # Progress: completed files' total size + in-flight new bytes
            total_progress = min(
                self.completed_bytes + self.new_bytes,
                self.total_bytes,
            )
            remaining = max(self.total_bytes - total_progress, 0)
            eta = remaining / max(speed, 1)
            pct = total_progress / max(self.total_bytes, 1) * 100
            return (
                f"[Overall] {self.completed_files}/{self.total_files} files | "
                f"{total_progress / 1e9:.2f}/{self.total_bytes / 1e9:.2f} GB ({pct:.0f}%) | "
                f"{speed / 1e6:.1f} MB/s | ETA {format_duration(eta)}"
            )


# ---------------------------------------------------------------------------
# Databento manifest loading
# ---------------------------------------------------------------------------


def load_databento_manifest(manifest_path: Path) -> list[dict]:
    """Load Databento's native manifest.json and return downloadable file info.

    Databento's manifest contains per-file entries with filename, size, SHA-256
    hash, and download URLs (both HTTPS and FTP). This function extracts only
    .dbn.zst data files, ignoring metadata files (condition.json, metadata.json).

    Args:
        manifest_path: Path to Databento's manifest.json

    Returns:
        List of dicts with keys: filename, size, hash, https_url

    Raises:
        FileNotFoundError: If manifest_path does not exist
        ValueError: If manifest format is invalid
    """
    with open(manifest_path) as f:
        data = json.load(f)

    if "files" not in data:
        raise ValueError(
            f"Invalid Databento manifest: missing 'files' key in {manifest_path}"
        )

    file_list = []
    for entry in data["files"]:
        filename = entry.get("filename", "")
        if not filename.endswith(".dbn.zst"):
            continue

        urls = entry.get("urls", {})
        https_url = urls.get("https", "")
        if not https_url:
            raise ValueError(
                f"No HTTPS URL for file {filename} in Databento manifest"
            )

        try:
            size = entry["size"]
            hash_str = entry["hash"]
        except KeyError as e:
            raise ValueError(
                f"Manifest entry for '{filename}' missing required key: {e}"
            ) from e

        file_list.append({
            "filename": filename,
            "size": size,
            "hash": hash_str,
            "https_url": https_url,
        })

    return file_list


def fetch_file_list(api_key: str, job_id: str) -> list[dict]:
    """Fetch file manifest from Databento batch.list_files API.

    Calls the Databento Historical API to get the authoritative file list
    for a batch job. Returns the same format as load_databento_manifest().

    Args:
        api_key: Databento API key (format: db-XXXXX)
        job_id: Batch job ID (e.g., "OPRA-20260305-FP53NRH898")

    Returns:
        List of dicts with keys: filename, size, hash, https_url

    Raises:
        requests.HTTPError: If API request fails
        ValueError: If response format is unexpected
    """
    url = f"{DATABENTO_API_BASE}/v{DATABENTO_API_VERSION}/batch.list_files"
    resp = requests.get(
        url,
        params={"job_id": job_id},
        auth=(api_key, ""),
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    resp.raise_for_status()

    entries = resp.json()
    if not isinstance(entries, list):
        raise ValueError(
            f"Unexpected response from batch.list_files: expected list, got {type(entries).__name__}"
        )

    file_list = []
    for entry in entries:
        filename = entry.get("filename", "")
        if not filename.endswith(".dbn.zst"):
            continue

        urls = entry.get("urls", {})
        https_url = urls.get("https", "")
        if not https_url:
            raise ValueError(f"No HTTPS URL for file {filename}")

        try:
            size = entry["size"]
            hash_str = entry["hash"]
        except KeyError as e:
            raise ValueError(
                f"API response entry for '{filename}' missing required key: {e}"
            ) from e

        file_list.append({
            "filename": filename,
            "size": size,
            "hash": hash_str,
            "https_url": https_url,
        })

    return file_list


# ---------------------------------------------------------------------------
# Single file download
# ---------------------------------------------------------------------------


def _parse_expected_hash(hash_str: str) -> tuple[str, str]:
    """Parse 'algo:hex' hash string from Databento manifest.

    Args:
        hash_str: Hash string like 'sha256:abcdef...'

    Returns:
        Tuple of (algorithm, hex_digest). Algorithm is lowered.

    Raises:
        ValueError: If format is unexpected
    """
    if ":" not in hash_str:
        raise ValueError(f"Unexpected hash format (missing ':'): {hash_str}")
    algo, hex_digest = hash_str.split(":", 1)
    return algo.lower(), hex_digest


def download_file(
    api_key: str,
    url: str,
    local_path: Path,
    expected_size: int,
    expected_hash: str,
    file_index: int = 0,
    total_files: int = 0,
    progress: DownloadProgress | None = None,
    http_timeout: tuple[int, int] = DEFAULT_HTTP_TIMEOUT,
) -> tuple[str, bool, str, str]:
    """Download a single file with atomic write, resume, and streaming SHA-256.

    Safety guarantees:
        - Writes to a .downloading temp file; renames only after BOTH size
          and SHA-256 verification pass against Databento's authoritative hash.
        - Hash mismatch is a hard error (file is deleted, not accepted).
        - Partial/corrupt temp files are cleaned up on failure.
        - Supports resume via HTTP Range headers for interrupted downloads,
          including across separate runs.
        - Verifies HTTP 206 response when Range header is sent; falls back
          to full download if server ignores Range.
        - Aborts and retries if speed drops below MIN_SPEED_BPS for
          MIN_SPEED_DURATION seconds.

    Args:
        api_key: Databento API key for HTTP Basic Auth
        url: HTTPS download URL
        local_path: Final local path for the file
        expected_size: Expected file size in bytes
        expected_hash: Expected hash string (format: 'sha256:hex...')
        file_index: 1-based index of file (for logging)
        total_files: Total number of files (for logging)
        progress: Optional aggregate progress tracker (thread-safe)
        http_timeout: (connect_timeout, read_timeout) in seconds

    Returns:
        Tuple of (filename, success, error_message, sha256_hex)
    """
    filename = local_path.name
    size_gb = expected_size / (1024 ** 3)
    prefix = f"[{file_index}/{total_files}]" if total_files > 0 else ""

    hash_algo, exp_hex = _parse_expected_hash(expected_hash)
    if hash_algo != "sha256":
        return (
            filename, False,
            f"Unsupported hash algorithm '{hash_algo}'. Only sha256 is supported.", "",
        )

    tmp_path = local_path.parent / (local_path.name + ".downloading")

    session = requests.Session()
    session.auth = (api_key, "")
    session.headers["User-Agent"] = f"databento-ingest/{__version__}"

    for attempt in range(MAX_RETRIES):
        hasher = hashlib.sha256()
        downloaded_bytes = 0
        resume_offset = 0
        start_time = time.time()
        last_progress_time = start_time
        speed_samples: deque[tuple[float, int]] = deque()
        headers = {}
        resp = None

        try:
            if tmp_path.exists():
                existing_size = tmp_path.stat().st_size
                if existing_size > 0 and existing_size < expected_size:
                    safe_print(
                        f"   {prefix} {filename}: Resuming from "
                        f"{existing_size / (1024**3):.2f} GB"
                    )
                    with open(tmp_path, "rb") as ef:
                        while True:
                            chunk = ef.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            hasher.update(chunk)
                    downloaded_bytes = existing_size
                    resume_offset = existing_size
                    headers["Range"] = f"bytes={existing_size}-"
                elif existing_size >= expected_size:
                    tmp_path.unlink()

            resp = session.get(
                url, headers=headers, stream=True, timeout=http_timeout
            )
            resp.raise_for_status()

            if resume_offset > 0 and resp.status_code != 206:
                safe_print(
                    f"   {prefix} {filename}: Server did not honor Range request "
                    f"(HTTP {resp.status_code}), restarting from beginning"
                )
                hasher = hashlib.sha256()
                downloaded_bytes = 0
                resume_offset = 0

            mode = "ab" if downloaded_bytes > 0 else "wb"
            with open(tmp_path, mode) as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    chunk_len = len(chunk)
                    f.write(chunk)
                    hasher.update(chunk)
                    downloaded_bytes += chunk_len
                    if progress:
                        progress.add_chunk(chunk_len)

                    current_time = time.time()
                    speed_samples.append((current_time, downloaded_bytes))
                    cutoff = current_time - SPEED_WINDOW
                    while speed_samples and speed_samples[0][0] < cutoff:
                        speed_samples.popleft()

                    if current_time - last_progress_time >= PROGRESS_INTERVAL:
                        pct = (
                            (downloaded_bytes / expected_size) * 100
                            if expected_size > 0
                            else 0
                        )
                        if len(speed_samples) >= 2:
                            oldest_t, oldest_b = speed_samples[0]
                            window_dt = current_time - oldest_t
                            window_db = downloaded_bytes - oldest_b
                            speed_mbs = (
                                (window_db / (1024 ** 2)) / window_dt
                                if window_dt > 0
                                else 0
                            )
                        else:
                            elapsed = current_time - start_time
                            speed_mbs = (
                                (downloaded_bytes / (1024 ** 2)) / elapsed
                                if elapsed > 0
                                else 0
                            )

                        remaining = expected_size - downloaded_bytes
                        eta_s = (
                            remaining / (speed_mbs * 1024 ** 2)
                            if speed_mbs > 0
                            else 0
                        )
                        safe_print(
                            f"   {prefix} {filename}: {pct:.0f}% "
                            f"({speed_mbs:.1f} MB/s, ETA {format_duration(eta_s)})"
                        )
                        if progress:
                            safe_print(f"   {progress.summary_line()}")
                        last_progress_time = current_time

                        # Min speed enforcement: abort if crawling
                        speed_bps = speed_mbs * 1024 * 1024
                        if (len(speed_samples) >= 2
                                and (current_time - speed_samples[0][0]) >= MIN_SPEED_DURATION
                                and speed_bps < MIN_SPEED_BPS):
                            raise ConnectionError(
                                f"Speed {speed_bps / 1000:.1f} KB/s below minimum "
                                f"{MIN_SPEED_BPS / 1000:.0f} KB/s for "
                                f"{MIN_SPEED_DURATION}s — aborting for retry"
                            )

            actual_size = tmp_path.stat().st_size
            if actual_size != expected_size:
                tmp_path.unlink(missing_ok=True)
                raise ValueError(
                    f"Size mismatch: expected {expected_size}, got {actual_size}"
                )

            sha256_hex = hasher.hexdigest()
            if sha256_hex != exp_hex:
                tmp_path.unlink(missing_ok=True)
                raise ValueError(
                    f"SHA-256 mismatch: expected {exp_hex[:16]}..., "
                    f"got {sha256_hex[:16]}..."
                )

            tmp_path.rename(local_path)
            if progress:
                progress.file_done(expected_size)

            elapsed = time.time() - start_time
            transferred = downloaded_bytes - resume_offset
            speed_mbs = (
                (transferred / (1024 ** 2)) / elapsed if elapsed > 0 else 0
            )
            safe_print(
                f"   OK {prefix} {filename} "
                f"({size_gb:.2f} GB @ {speed_mbs:.1f} MB/s) "
                f"sha256={sha256_hex[:16]}... verified"
            )
            session.close()
            return (filename, True, "", sha256_hex)

        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.HTTPError,
            ConnectionResetError,
            TimeoutError,
            OSError,
            ValueError,
        ) as e:
            # Reset progress for bytes counted in this failed attempt
            new_this_attempt = downloaded_bytes - resume_offset
            if progress and new_this_attempt > 0:
                progress.reset_file_progress(new_this_attempt)

            if tmp_path.exists() and not isinstance(e, ValueError):
                pass  # preserve partial for resume
            elif tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

            delay = RETRY_DELAY_BASE * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                safe_print(
                    f"   WARN {prefix} {filename}: "
                    f"Retry {attempt + 1}/{MAX_RETRIES} in {delay}s "
                    f"({type(e).__name__}: {e})"
                )
                time.sleep(delay)
            else:
                safe_print(
                    f"   FAIL {prefix} {filename}: "
                    f"Failed after {MAX_RETRIES} attempts"
                )
                tmp_path.unlink(missing_ok=True)
                session.close()
                return (filename, False, str(e), "")
        finally:
            if resp is not None:
                resp.close()

    session.close()
    return (filename, False, "Max retries exceeded", "")


# ---------------------------------------------------------------------------
# Batch download orchestrator
# ---------------------------------------------------------------------------


def verify_downloads(
    manifest_path: Path,
    output_dir: Path,
) -> tuple[list[str], list[str]]:
    """Verify SHA-256 of downloaded files against Databento manifest.

    Recomputes SHA-256 for each file in the manifest and compares
    against the authoritative hash. Reports per-file pass/fail.

    Args:
        manifest_path: Path to Databento manifest.json
        output_dir: Directory containing downloaded files

    Returns:
        Tuple of (passed_files, failed_files)
    """
    file_list = load_databento_manifest(manifest_path)
    passed: list[str] = []
    failed: list[str] = []

    print(f"Verifying {len(file_list)} files against manifest...")

    for i, finfo in enumerate(file_list, 1):
        filename = finfo["filename"]
        local_path = output_dir / filename
        expected_size = finfo["size"]
        algo, exp_hex = _parse_expected_hash(finfo["hash"])
        if algo != "sha256":
            print(
                f"   UNSUPPORTED HASH [{i}/{len(file_list)}] {filename}: "
                f"algorithm '{algo}' — only sha256 is supported"
            )
            failed.append(filename)
            continue

        if not local_path.exists():
            print(f"   MISSING [{i}/{len(file_list)}] {filename}")
            failed.append(filename)
            continue

        actual_size = local_path.stat().st_size
        if actual_size != expected_size:
            print(
                f"   SIZE MISMATCH [{i}/{len(file_list)}] {filename}: "
                f"expected {expected_size}, got {actual_size}"
            )
            failed.append(filename)
            continue

        # Stream SHA-256
        hasher = hashlib.sha256()
        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)

        actual_hex = hasher.hexdigest()
        if actual_hex != exp_hex:
            print(
                f"   HASH MISMATCH [{i}/{len(file_list)}] {filename}: "
                f"expected {exp_hex[:16]}..., got {actual_hex[:16]}..."
            )
            failed.append(filename)
        else:
            print(f"   OK [{i}/{len(file_list)}] {filename}")
            passed.append(filename)

    print(f"\nVerification: {len(passed)} passed, {len(failed)} failed "
          f"out of {len(file_list)} files")
    return passed, failed


def download_job(
    api_key: str,
    job_id: str,
    output_dir: Path,
    symbol: str,
    manifest_path: Path | None = None,
    dry_run: bool = False,
    parallel: int = PARALLEL_DOWNLOADS,
    dataset: str = "",
    schema: str | None = None,
    http_timeout: tuple[int, int] = DEFAULT_HTTP_TIMEOUT,
    estimated_speed_mbs: float = DEFAULT_ESTIMATED_SPEED_MBS,
) -> list[str]:
    """Download all files for a Databento batch job via HTTPS.

    Loads the file manifest (from local file or API), then downloads each
    .dbn.zst file in parallel with full safety guarantees.

    Args:
        api_key: Databento API key
        job_id: Batch job ID
        output_dir: Local directory to save files
        symbol: Symbol name for our manifest metadata
        manifest_path: Optional path to local Databento manifest.json.
            If None, fetches file list from Databento API.
        dry_run: If True, only list files without downloading
        parallel: Number of parallel downloads (default: PARALLEL_DOWNLOADS = 4)
        dataset: Databento dataset identifier for our manifest metadata
        schema: Data schema for our manifest metadata

    Returns:
        List of downloaded file paths
    """
    print(f"Job: {job_id or '(from manifest)'}")
    if manifest_path and manifest_path.exists():
        print(f"Loading file list from {manifest_path}")
        file_list = load_databento_manifest(manifest_path)
    elif manifest_path and not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest file not found: {manifest_path}. "
            f"Ensure the manifest bundle has been copied to the expected location."
        )
    elif not job_id:
        raise ValueError(
            "No manifest file and no job_id specified. "
            "Provide either a manifest_path or a job_id in the config."
        )
    else:
        print("Fetching file list from Databento API...")
        file_list = fetch_file_list(api_key, job_id)

    if not file_list:
        print("No .dbn.zst files found in job manifest.")
        return []

    file_list.sort(key=lambda x: x["filename"])
    total_size = sum(f["size"] for f in file_list)

    print(f"\nFound {len(file_list)} data files:")
    print(f"   Total size: {total_size / (1024**3):.2f} GB")
    print(f"   Parallel connections: {parallel}")
    print(f"   Protocol: HTTPS (API key auth)")

    if dry_run:
        print("\nDRY RUN — Files that would be downloaded:")
        for f in file_list:
            size_gb = f["size"] / (1024 ** 3)
            print(f"   - {f['filename']} ({size_gb:.2f} GB)")
        est_time = total_size / max(estimated_speed_mbs * 1024 ** 2, 1)
        print(f"\n   Estimated download time at ~{estimated_speed_mbs:.0f} MB/s: {format_duration(est_time)}")
        return []

    # 1. Determine which files need downloading
    downloaded: list[str] = []
    to_download: list[dict] = []

    for i, finfo in enumerate(file_list, 1):
        filename = finfo["filename"]
        expected_size = finfo["size"]
        size_gb = expected_size / (1024 ** 3)
        local_path = output_dir / filename

        if local_path.exists():
            local_size = local_path.stat().st_size
            if local_size == expected_size:
                print(f"   Skipping {filename} (already exists, {size_gb:.2f} GB)")
                downloaded.append(str(local_path))
                continue
            else:
                print(
                    f"   Incomplete {filename} "
                    f"({local_size} vs {expected_size}), will re-download"
                )
                local_path.unlink()

        to_download.append({**finfo, "local_path": local_path, "index": i})

    if not to_download:
        print(f"\nAll {len(file_list)} files already downloaded!")
        return downloaded

    remaining_size = sum(item["size"] for item in to_download)

    # 2. Pre-flight checks (using remaining_size, not total_size)
    output_dir.mkdir(parents=True, exist_ok=True)

    disk_usage = shutil.disk_usage(output_dir)
    free_bytes = disk_usage.free
    if remaining_size > free_bytes * DISK_SPACE_SAFETY_MARGIN:
        print(
            f"\nERROR: Insufficient disk space. "
            f"Need {remaining_size / (1024**3):.1f} GB for remaining files, "
            f"available {free_bytes / (1024**3):.1f} GB "
            f"(with {(1 - DISK_SPACE_SAFETY_MARGIN) * 100:.0f}% safety margin)"
        )
        return []
    print(
        f"   Disk space: {free_bytes / (1024**3):.1f} GB free "
        f"(need {remaining_size / (1024**3):.1f} GB) — OK"
    )

    # Clean stale temps only for files NOT in our download queue
    # (preserves resumable partial downloads for files we're about to download)
    to_download_temps = {item["filename"] + ".downloading" for item in to_download}
    for stale_tmp in output_dir.glob("*.downloading"):
        if stale_tmp.name not in to_download_temps:
            print(f"   Cleaning stale temp file: {stale_tmp.name}")
            stale_tmp.unlink()

    # 3. Start parallel download
    est_time = remaining_size / max(estimated_speed_mbs * 1024 ** 2, 1)
    print(
        f"\nDownloading {len(to_download)} files "
        f"({remaining_size / (1024**3):.2f} GB) "
        f"with {parallel} parallel HTTPS connections..."
    )
    print(f"   Estimated time at ~{estimated_speed_mbs:.0f} MB/s: {format_duration(est_time)}")
    start_time = time.time()

    failed: list[str] = []
    checksums: dict[str, str] = {}
    progress = DownloadProgress(
        total_files=len(to_download),
        total_bytes=remaining_size,
    )

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {}
        for item in to_download:
            future = executor.submit(
                download_file,
                api_key=api_key,
                url=item["https_url"],
                local_path=item["local_path"],
                expected_size=item["size"],
                expected_hash=item["hash"],
                file_index=item["index"],
                total_files=len(file_list),
                progress=progress,
                http_timeout=http_timeout,
            )
            futures[future] = item

        for future in as_completed(futures):
            item = futures[future]
            try:
                filename, success, error_msg, sha256_hex = future.result()
                if success:
                    downloaded.append(str(item["local_path"]))
                    if sha256_hex:
                        checksums[filename] = sha256_hex
                else:
                    failed.append(filename)
            except Exception as e:
                safe_print(f"   FAIL {item['filename']}: Unexpected error: {e}")
                failed.append(item["filename"])

    # 4. Summary (speed based only on newly downloaded bytes)
    elapsed = time.time() - start_time
    newly_downloaded_size = sum(
        item["size"] for item in to_download
        if str(item["local_path"]) in downloaded
    )
    avg_speed = (
        (newly_downloaded_size / (1024 ** 2)) / elapsed if elapsed > 0 else 0
    )

    print(f"\n{'=' * 60}")
    print(f"Download Summary:")
    print(f"   Successful: {len(downloaded)}/{len(file_list)}")
    print(f"   Time: {format_duration(elapsed)}")
    print(f"   Average speed: {avg_speed:.1f} MB/s ({avg_speed * 8:.0f} Mbps)")

    if failed:
        print(f"   Failed: {len(failed)}")
        for f in failed:
            print(f"      - {f}")
        print(f"\n   To retry failed files, run the command again.")
        print(f"   Already downloaded files will be skipped automatically.")

    # 5. Create our manifest
    if downloaded:
        date_range = extract_date_range([Path(f).name for f in downloaded])
        create_manifest(
            output_dir=output_dir,
            symbol=symbol,
            source="https",
            date_range=date_range,
            files=[Path(f).name for f in downloaded],
            metadata={
                "job_id": job_id,
                "total_size_bytes": total_size,
                "failed_files": failed,
                "download_speed_mbps": round(avg_speed, 2),
                "download_elapsed_seconds": round(elapsed, 1),
                "parallel_connections": parallel,
            },
            dataset=dataset,
            schema=schema,
            checksums=checksums,
        )

    return downloaded
