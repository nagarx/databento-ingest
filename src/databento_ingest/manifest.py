"""Manifest creation, reading, and validation for downloaded datasets.

Every downloaded dataset produces a manifest.json recording provenance,
file inventory, SHA-256 checksums, and download metadata. It is primarily
a PROVENANCE / record artifact: pipeline modules (feature extractors,
reconstructor, profilers) do NOT read it — they consume the .dbn.zst files
directly. Its one known downstream consumer is the monorepo-root
completeness checker ``scripts/validate_dataset.py``, which parses the
top-level ``date_range`` and ``metadata.failed_files`` fields (graceful
``.get()`` fallbacks) — treat those two field names as a soft contract.
Integrity re-checks go through the ``verify`` subcommand (against the
Databento-provided job manifest) or an independent SHA256SUMS, not this
file.

Manifest schema version: 1.3 (added ingest_tool_version + databento_api_version
metadata fields for multi-year reproducibility; non-breaking additive change).

Schema reference: see CODEBASE.md §Manifest Schema (v1.3) + §Downstream
Boundary / Consumers.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from hft_contracts.atomic_io import atomic_write_json  # #PY-371 SSoT (Class A)

MANIFEST_SCHEMA_VERSION = "1.3"
MANIFEST_FILENAME = "manifest.json"


def create_manifest(
    output_dir: Path,
    symbol: str,
    source: str,
    date_range: str,
    files: list[str],
    metadata: dict,
    dataset: str = "XNAS.ITCH",
    schema: str | None = None,
    checksums: Optional[dict[str, str]] = None,
) -> Path:
    """Create a manifest.json for a downloaded dataset.

    Per RULE.md §8 (Data Integrity): record diagnostics for all artifacts.

    Args:
        output_dir: Directory to write manifest into
        symbol: Trading symbol (e.g., "NVDA")
        source: Download method (e.g., "https", "api_batch")
        date_range: Human-readable date range string (e.g., "2025-11-13 to 2025-11-25")
        files: List of downloaded filenames
        metadata: Additional metadata dict (job_id, speed, etc.)
        dataset: Databento dataset identifier (e.g., "XNAS.ITCH", "OPRA")
        schema: Data schema (e.g., "mbo", "cmbp-1"). None if not specified.
        checksums: Dict of {filename: sha256_hex} for integrity verification

    Returns:
        Path to the created manifest.json
    """
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "symbol": symbol,
        "dataset": dataset,
        "source": "databento",
        "download_method": source,
        "date_range": date_range,
        "download_timestamp": datetime.now(timezone.utc).isoformat(),
        "file_count": len(files),
        "files": sorted(files),
        "checksums": checksums or {},
        "metadata": metadata,
    }
    if schema is not None:
        manifest["schema"] = schema

    # #PY-371: atomic_write_json SSoT (tmp + fsync + os.replace + cleanup).
    # Replaces prior manual rename pattern (lacked fsync; race on FS journal flush).
    # Per hft-rules.md §8: never silently leave diagnostic-less artifacts on disk —
    # SSoT primitive handles this via BaseException-safe cleanup.
    #
    # Pre-validate JSON-serializability BEFORE atomic_write_json — required because
    # atomic_write_json uses ``default=str`` which silently coerces non-serializable
    # types (set, custom objects, etc.) into their repr-strings. §8 fail-loud-on-
    # bad-caller-data is preserved here by raising TypeError before the SSoT call.
    manifest_path = output_dir / MANIFEST_FILENAME
    json.dumps(manifest)  # raises TypeError on non-JSON-serializable values (§8)
    atomic_write_json(manifest_path, manifest)

    print(f"  Created manifest: {manifest_path}")
    return manifest_path


def read_manifest(manifest_path: Path) -> dict:
    """Read and return a manifest.json.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Parsed manifest dict

    Raises:
        FileNotFoundError: If manifest does not exist
        json.JSONDecodeError: If manifest is not valid JSON
    """
    with open(manifest_path) as f:
        return json.load(f)


def validate_manifest(manifest: dict) -> list[str]:
    """Validate a manifest dict against the expected schema.

    Checks for required fields and correct types. Does NOT verify file
    existence or checksums (that's the caller's responsibility).

    Args:
        manifest: Parsed manifest dict

    Returns:
        List of validation error strings (empty if valid)
    """
    errors: list[str] = []
    required_fields = [
        ("schema_version", str),
        ("symbol", str),
        ("dataset", str),
        ("source", str),
        ("download_method", str),
        ("date_range", str),
        ("download_timestamp", str),
        ("file_count", int),
        ("files", list),
    ]

    for field, expected_type in required_fields:
        if field not in manifest:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(manifest[field], expected_type):
            errors.append(
                f"Field '{field}' has type {type(manifest[field]).__name__}, "
                f"expected {expected_type.__name__}"
            )

    if "files" in manifest and "file_count" in manifest:
        if isinstance(manifest["files"], list) and isinstance(manifest["file_count"], int):
            if len(manifest["files"]) != manifest["file_count"]:
                errors.append(
                    f"file_count ({manifest['file_count']}) does not match "
                    f"len(files) ({len(manifest['files'])})"
                )

    if "checksums" in manifest and not isinstance(manifest["checksums"], dict):
        errors.append(
            f"Field 'checksums' has type {type(manifest['checksums']).__name__}, "
            f"expected dict"
        )

    return errors


def extract_date_range(filenames: list[str]) -> str:
    """Extract date range from Databento filenames.

    Handles both XNAS and OPRA naming conventions:
        - xnas-itch-20250929.mbo.dbn.zst
        - opra-pillar-20251113.cmbp-1.dbn.zst

    Args:
        filenames: List of Databento data filenames

    Returns:
        Formatted date range string like '2025-09-29 to 2025-11-25',
        or 'unknown' if no dates could be extracted
    """
    dates: list[str] = []
    for f in filenames:
        parts = f.split("-")
        for part in parts:
            date_part = part.split(".")[0]
            if len(date_part) == 8 and date_part.isdigit():
                dates.append(date_part)
                break

    if dates:
        dates.sort()
        start = f"{dates[0][:4]}-{dates[0][4:6]}-{dates[0][6:]}"
        end = f"{dates[-1][:4]}-{dates[-1][4:6]}-{dates[-1][6:]}"
        return f"{start} to {end}"
    return "unknown"
