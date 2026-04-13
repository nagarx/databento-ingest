"""Databento API batch operations: submit jobs, list jobs, and merge datasets.

Job submission and listing require the optional `databento` pip package.
Merge operations work without external dependencies.

Batch download is handled by downloader.py via HTTPS (not this module).
"""

import shutil
from pathlib import Path
from typing import Optional


def submit_batch_job(
    api_key: str,
    dataset: str,
    symbol: str,
    start: str,
    end: str,
    output_dir: Path,
    schema: str = "mbo",
) -> str:
    """Submit a new batch job via Databento API.

    Requires: pip install databento

    Args:
        api_key: Databento API key
        dataset: Dataset identifier (e.g., "XNAS.ITCH", "OPRA")
        symbol: Symbol to download
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        output_dir: Output directory for downloaded files
        schema: Data schema (default: "mbo")

    Returns:
        Job ID string
    """
    try:
        import databento as db
    except ImportError:
        raise ImportError(
            "databento package not installed. Install with: pip install databento"
        )

    print(f"Submitting batch job...")
    print(f"   Dataset: {dataset}")
    print(f"   Symbol: {symbol}")
    print(f"   Date range: {start} to {end}")
    print(f"   Schema: {schema}")

    client = db.Historical(api_key)

    job = client.batch.submit_job(
        dataset=dataset,
        start=start,
        end=end,
        symbols=symbol,
        schema=schema,
        split_duration="day",
        encoding="dbn",
        compression="zstd",
    )

    job_id = job["id"]
    print(f"\nJob submitted!")
    print(f"   Job ID: {job_id}")
    print(f"   Status: {job.get('state', 'pending')}")
    print(f"\nTo download when ready, run:")
    print(f'   python -m databento_ingest download-job \\')
    print(f'       --job-id "{job_id}" \\')
    print(f'       --output-dir "{output_dir}" \\')
    print(f'       --symbol "{symbol}" --dataset "{dataset}"')

    return job_id


def list_jobs(api_key: str, status_filter: Optional[str] = None):
    """List batch jobs.

    Args:
        api_key: Databento API key
        status_filter: Optional filter (pending, processing, done, expired)
    """
    try:
        import databento as db
    except ImportError:
        raise ImportError(
            "databento package not installed. Install with: pip install databento"
        )

    client = db.Historical(api_key)

    jobs = client.batch.list_jobs(status_filter)

    if not jobs:
        print("No jobs found.")
        return

    print(f"\nFound {len(jobs)} job(s):\n")

    for job in jobs:
        job_id = job.get("id", "unknown")
        state = job.get("state", "unknown")
        ds = job.get("dataset", "unknown")
        symbols = job.get("symbols", [])
        start = job.get("start", "")
        end = job.get("end", "")

        status_icon = {
            "pending": "[PENDING]",
            "processing": "[PROCESSING]",
            "done": "[DONE]",
            "expired": "[EXPIRED]",
            "failed": "[FAILED]",
        }.get(state, "[?]")

        print(f"{status_icon} {job_id}")
        print(f"   Status: {state}")
        print(f"   Dataset: {ds}")
        print(f"   Symbols: {', '.join(symbols) if symbols else 'N/A'}")
        print(f"   Range: {start} to {end}")
        print()


def merge_datasets(
    source_dir: Path,
    target_dir: Path,
    dry_run: bool = False,
) -> int:
    """Merge .dbn.zst files from source into target directory.

    Args:
        source_dir: Directory with new files
        target_dir: Directory to merge into
        dry_run: If True, only show what would be done

    Returns:
        Number of files merged
    """
    if not source_dir.exists():
        print(f"Error: Source directory does not exist: {source_dir}")
        return 0

    target_dir.mkdir(parents=True, exist_ok=True)

    source_files = list(source_dir.glob("*.dbn.zst"))

    if not source_files:
        print(f"Warning: No .dbn.zst files found in {source_dir}")
        return 0

    print(f"\nMerging {len(source_files)} files from {source_dir} to {target_dir}")

    merged_count = 0
    skipped_count = 0

    for src_file in sorted(source_files):
        dst_file = target_dir / src_file.name

        if dst_file.exists():
            if dst_file.stat().st_size == src_file.stat().st_size:
                print(f"   Skipping {src_file.name} (already exists)")
                skipped_count += 1
                continue
            else:
                print(f"   Size mismatch for {src_file.name}, will overwrite")

        if dry_run:
            print(f"   Would copy: {src_file.name}")
        else:
            print(f"   Copying: {src_file.name}")
            shutil.copy2(src_file, dst_file)

        merged_count += 1

    print(f"\nMerged {merged_count} files, skipped {skipped_count}")
    return merged_count
