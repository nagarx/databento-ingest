"""CLI entry point for the databento-ingest module.

Subcommands:
    download      Config-driven download (reads configs/datasets/*.toml)
    download-job  Download from a batch job by job ID (direct CLI args)
    verify        Verify downloaded files against manifest SHA-256 checksums
    batch         Submit new batch request via Databento API
    list-jobs     List batch jobs
    merge         Merge files between directories

All download operations use HTTPS with Databento API key authentication.
Credentials are read from credentials.toml (gitignored).
CLI --api-key overrides credentials.toml.

Usage:
    python -m databento_ingest download --config configs/datasets/opra_nvda_cmbp1_nov2025.toml
    python -m databento_ingest download-job --job-id "OPRA-20260305-FP53NRH898" --output-dir "data/..."
"""

import argparse
import sys
from pathlib import Path

from databento_ingest.batch import (
    list_jobs,
    merge_datasets,
    submit_batch_job,
)
from databento_ingest.config import (
    CONFIGS_DIR,
    Credentials,
    load_credentials,
    load_dataset_config,
)
from databento_ingest.downloader import (
    DEFAULT_ESTIMATED_SPEED_MBS,
    DEFAULT_HTTP_TIMEOUT,
    PARALLEL_DOWNLOADS,
    download_job,
    verify_downloads,
)


def _resolve_credentials(args: argparse.Namespace) -> Credentials:
    """Merge credentials.toml with CLI overrides.

    CLI args take precedence over credentials.toml values.
    """
    creds = load_credentials()

    if hasattr(args, "api_key") and args.api_key:
        creds.api_key = args.api_key

    return creds


def _require_api_key(creds: Credentials) -> str:
    """Extract API key or exit with error."""
    if not creds.api_key:
        print(
            "Error: API key required. Provide via credentials.toml "
            "or --api-key."
        )
        print("   See: https://databento.com/portal/keys")
        sys.exit(1)
    return creds.api_key


def cmd_download(args: argparse.Namespace):
    """Config-driven download from TOML specification."""
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = CONFIGS_DIR.parent / config_path

    try:
        config = load_dataset_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    creds = _resolve_credentials(args)
    api_key = _require_api_key(creds)

    output_dir = Path(config.storage.output_dir)
    if not output_dir.is_absolute():
        module_root = CONFIGS_DIR.parent.parent
        output_dir = module_root / config.storage.output_dir

    manifest_path = None
    if config.source.manifest_path:
        mp = Path(config.source.manifest_path)
        if not mp.is_absolute():
            manifest_path = CONFIGS_DIR.parent / mp
        else:
            manifest_path = mp

    http_timeout = (
        getattr(args, "connect_timeout", DEFAULT_HTTP_TIMEOUT[0]),
        getattr(args, "read_timeout", DEFAULT_HTTP_TIMEOUT[1]),
    )
    estimated_speed = getattr(args, "estimated_speed", DEFAULT_ESTIMATED_SPEED_MBS)

    downloaded = download_job(
        api_key=api_key,
        job_id=config.source.job_id,
        output_dir=output_dir,
        symbol=config.dataset.symbol,
        manifest_path=manifest_path,
        dry_run=args.dry_run,
        parallel=config.download.parallel,
        dataset=config.dataset.name,
        schema=config.dataset.schema or None,
        http_timeout=http_timeout,
        estimated_speed_mbs=estimated_speed,
    )
    if downloaded:
        print(f"\nSuccessfully downloaded {len(downloaded)} files to {output_dir}")


def cmd_download_job(args: argparse.Namespace):
    """Download from a batch job by job ID."""
    creds = _resolve_credentials(args)
    api_key = _require_api_key(creds)

    output_dir = Path(args.output_dir)
    manifest_path = Path(args.manifest) if args.manifest else None

    downloaded = download_job(
        api_key=api_key,
        job_id=args.job_id,
        output_dir=output_dir,
        symbol=args.symbol,
        manifest_path=manifest_path,
        dry_run=args.dry_run,
        parallel=args.parallel,
        dataset=args.dataset,
    )
    if downloaded:
        print(f"\nSuccessfully downloaded {len(downloaded)} files to {output_dir}")


def cmd_batch(args: argparse.Namespace):
    """Submit new batch request via API."""
    creds = _resolve_credentials(args)
    api_key = _require_api_key(creds)

    try:
        submit_batch_job(
            api_key=api_key,
            dataset=args.dataset,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            output_dir=Path(args.output_dir),
            schema=args.schema,
        )
    except ImportError as e:
        print(f"Error: {e}")
        sys.exit(1)


def cmd_list_jobs(args: argparse.Namespace):
    """List batch jobs."""
    creds = _resolve_credentials(args)
    api_key = _require_api_key(creds)

    try:
        list_jobs(api_key=api_key, status_filter=args.status)
    except ImportError as e:
        print(f"Error: {e}")
        sys.exit(1)


def cmd_verify(args: argparse.Namespace):
    """Verify downloaded files against Databento manifest checksums."""
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = CONFIGS_DIR.parent / config_path

    try:
        config = load_dataset_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)

    output_dir = Path(config.storage.output_dir)
    if not output_dir.is_absolute():
        module_root = CONFIGS_DIR.parent.parent
        output_dir = module_root / config.storage.output_dir

    manifest_path = None
    if config.source.manifest_path:
        mp = Path(config.source.manifest_path)
        if not mp.is_absolute():
            manifest_path = CONFIGS_DIR.parent / mp
        else:
            manifest_path = mp

    if not manifest_path or not manifest_path.exists():
        print(f"Error: Manifest not found at {manifest_path}")
        sys.exit(1)

    passed, failed = verify_downloads(manifest_path, output_dir)
    sys.exit(1 if failed else 0)


def cmd_merge(args: argparse.Namespace):
    """Merge datasets between directories."""
    merge_datasets(
        source_dir=Path(args.source),
        target_dir=Path(args.target),
        dry_run=args.dry_run,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="databento-ingest",
        description="Databento data acquisition — high-throughput HTTPS downloads",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Config-driven download (preferred)
  python -m databento_ingest download \\
      --config configs/datasets/opra_nvda_cmbp1_nov2025.toml

  # Direct download by job ID
  python -m databento_ingest download-job \\
      --job-id "OPRA-20260305-FP53NRH898" \\
      --output-dir "data/OPRA/NVDA/cmbp1_2025-10-29_to_2025-11-24" \\
      --symbol NVDA --dataset OPRA

  # Credentials are read from databento-ingest/credentials.toml
  # CLI --api-key overrides credentials.toml
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # --- download (config-driven) ---
    dl_parser = subparsers.add_parser(
        "download", help="Config-driven download (reads dataset TOML)"
    )
    dl_parser.add_argument(
        "--config", required=True,
        help="Path to dataset config TOML (relative to databento-ingest/ or absolute)",
    )
    dl_parser.add_argument("--dry-run", action="store_true", help="List files without downloading")
    dl_parser.add_argument("--api-key", default="", help="Override credentials.toml API key")
    dl_parser.add_argument(
        "--connect-timeout", type=int, default=DEFAULT_HTTP_TIMEOUT[0],
        help=f"HTTP connect timeout in seconds (default: {DEFAULT_HTTP_TIMEOUT[0]})",
    )
    dl_parser.add_argument(
        "--read-timeout", type=int, default=DEFAULT_HTTP_TIMEOUT[1],
        help=f"HTTP read timeout in seconds (default: {DEFAULT_HTTP_TIMEOUT[1]})",
    )
    dl_parser.add_argument(
        "--estimated-speed", type=float, default=DEFAULT_ESTIMATED_SPEED_MBS,
        help=f"Estimated download speed in MB/s for time estimates (default: {DEFAULT_ESTIMATED_SPEED_MBS})",
    )

    # --- download-job (direct by job ID) ---
    job_parser = subparsers.add_parser(
        "download-job", help="Download from a batch job by job ID"
    )
    job_parser.add_argument("--job-id", required=True, help="Databento batch job ID")
    job_parser.add_argument("--output-dir", required=True, help="Output directory")
    job_parser.add_argument("--symbol", default="NVDA", help="Symbol name (default: NVDA)")
    job_parser.add_argument(
        "--dataset", default="XNAS.ITCH",
        help="Databento dataset identifier (default: XNAS.ITCH)",
    )
    job_parser.add_argument(
        "--parallel", type=int, default=PARALLEL_DOWNLOADS,
        help=f"Parallel download connections (default: {PARALLEL_DOWNLOADS})",
    )
    job_parser.add_argument(
        "--manifest", default="",
        help="Path to local Databento manifest.json (skips API call)",
    )
    job_parser.add_argument("--dry-run", action="store_true", help="List files without downloading")
    job_parser.add_argument("--api-key", default="", help="Override credentials.toml API key")

    # --- batch ---
    batch_parser = subparsers.add_parser("batch", help="Submit new batch request via API")
    batch_parser.add_argument("--api-key", default="", help="Databento API key")
    batch_parser.add_argument("--dataset", default="XNAS.ITCH", help="Dataset (default: XNAS.ITCH)")
    batch_parser.add_argument("--symbol", required=True, help="Symbol to download")
    batch_parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    batch_parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    batch_parser.add_argument("--output-dir", required=True, help="Output directory")
    batch_parser.add_argument("--schema", default="mbo", help="Schema (default: mbo)")

    # --- list-jobs ---
    list_parser = subparsers.add_parser("list-jobs", help="List batch jobs")
    list_parser.add_argument("--api-key", default="", help="Databento API key")
    list_parser.add_argument(
        "--status", choices=["pending", "processing", "done", "expired"],
        help="Filter by status",
    )

    # --- verify ---
    verify_parser = subparsers.add_parser(
        "verify", help="Verify downloaded files against manifest checksums"
    )
    verify_parser.add_argument(
        "--config", required=True,
        help="Path to dataset config TOML (same as download command)",
    )

    # --- merge ---
    merge_parser = subparsers.add_parser("merge", help="Merge files between directories")
    merge_parser.add_argument("--source", required=True, help="Source directory")
    merge_parser.add_argument("--target", required=True, help="Target directory")
    merge_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "download": cmd_download,
        "download-job": cmd_download_job,
        "batch": cmd_batch,
        "list-jobs": cmd_list_jobs,
        "verify": cmd_verify,
        "merge": cmd_merge,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
