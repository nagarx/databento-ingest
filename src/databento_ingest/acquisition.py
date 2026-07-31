"""Typed, cost-capped Databento batch acquisition planning.

The request fingerprint covers the complete provider query, batch file
customizations, cost cap, and symbol-roster provenance. Credentials are passed
only to client construction and are never members of serializable types.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import stat
import tempfile
import tomllib
import fcntl
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, fields, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Final


_DATETIME_PARSER = datetime
PLAN_MAX_AGE = timedelta(minutes=15)
PLAN_SCHEMA_VERSION = "1.0"
RECEIPT_SCHEMA_VERSION = "1.1"
SUBMISSION_STATE_SCHEMA_VERSION = "1.1"
ABSOLUTE_MAX_COST_USD: Final = Decimal("1.00")
MAX_REQUEST_SYMBOLS: Final = 2_000
MAX_SYMBOL_UTF8_BYTES: Final = 128
MAX_JOB_ID_UTF8_BYTES: Final = 256
MAX_STATE_REASON_UTF8_BYTES: Final = 128
MAX_SUBMISSION_JOURNAL_BYTES: Final = 1024 * 1024
CONDITION_COVERAGE_CLASSIFICATION: Final = "provider_rows_unverified"
_MAX_CANONICAL_UTC_TEXT: Final = "9999-12-31T23:59:59.999999Z"
_CAPACITY_EXHAUSTED_MESSAGE: Final = (
    "submission ledger capacity exhausted; automatic retry is disabled; "
    "preserve the journal and reconcile this identity manually before any "
    "further submission"
)
SAFE_SUBMIT_RESPONSE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "id",
        "job_id",
        "state",
        "cost_usd",
        "dataset",
        "symbols",
        "stype_in",
        "stype_out",
        "schema",
        "start",
        "end",
        "limit",
        "encoding",
        "compression",
        "pretty_px",
        "pretty_ts",
        "map_symbols",
        "split_symbols",
        "split_duration",
        "split_size",
        "delivery",
        "record_count",
        "billed_size",
        "actual_size",
        "package_size",
        "ts_received",
        "ts_queued",
        "ts_process_start",
        "ts_process_done",
        "ts_expiration",
    },
)


class AcquisitionRefused(ValueError):
    """Raised before submission when a request fails a safety guard."""


def _canonical_json(value: object) -> bytes:
    """Return deterministic UTF-8 JSON bytes for hashing and artifacts."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def write_canonical_json_no_clobber(path: Path, value: object) -> None:
    """Atomically create canonical JSON while refusing an existing target."""
    payload = _canonical_json(value) + b"\n"
    parent = path.parent
    if not parent.is_dir():
        raise FileNotFoundError(f"output directory does not exist: {parent}")
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temp_path, path)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temp_path.unlink(missing_ok=True)


def _require_section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    section = raw.get(name)
    if not isinstance(section, dict):
        raise ValueError(f"missing or invalid [{name}] section")
    return section


def _reject_unknown_keys(
    value: dict[str, Any],
    allowed: frozenset[str],
    label: str,
    noun: str,
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"unknown {label} {noun}: {', '.join(unknown)}")


@dataclass(frozen=True)
class BatchRequest:
    """Immutable Databento request plus spend and roster-provenance guards."""

    SCHEMAS: ClassVar[frozenset[str]] = frozenset(
        {
            "mbo",
            "mbp-1",
            "mbp-10",
            "tbbo",
            "trades",
            "ohlcv-1s",
            "ohlcv-1m",
            "ohlcv-1h",
            "ohlcv-1d",
            "definition",
            "statistics",
            "status",
            "imbalance",
            "ohlcv-eod",
            "cmbp-1",
            "cbbo-1s",
            "cbbo-1m",
            "tcbbo",
            "bbo-1s",
            "bbo-1m",
        },
    )
    STYPES: ClassVar[frozenset[str]] = frozenset(
        {
            "instrument_id",
            "raw_symbol",
            "smart",
            "continuous",
            "parent",
            "nasdaq_symbol",
            "cms_symbol",
            "isin",
            "us_code",
            "bbg_comp_id",
            "bbg_comp_ticker",
            "figi",
            "figi_ticker",
        },
    )
    ENCODINGS: ClassVar[frozenset[str]] = frozenset({"dbn", "csv", "json"})
    COMPRESSIONS: ClassVar[frozenset[str]] = frozenset({"none", "zstd"})
    SPLIT_DURATIONS: ClassVar[frozenset[str]] = frozenset(
        {"day", "week", "month", "year", "none"},
    )
    DELIVERIES: ClassVar[frozenset[str]] = frozenset({"download"})
    ROOT_SECTIONS: ClassVar[frozenset[str]] = frozenset(
        {"request", "guard", "provenance"},
    )
    REQUEST_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {
            "dataset",
            "schema",
            "start",
            "end",
            "symbols",
            "stype_in",
            "stype_out",
            "encoding",
            "compression",
            "pretty_px",
            "pretty_ts",
            "map_symbols",
            "split_symbols",
            "split_duration",
            "delivery",
        },
    )
    GUARD_FIELDS: ClassVar[frozenset[str]] = frozenset({"max_cost_usd"})
    PROVENANCE_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {"symbol_source_path", "symbol_source_sha256", "symbol_count"},
    )

    dataset: str
    schema: str
    start: str
    end: str
    symbols: tuple[str, ...]
    stype_in: str
    stype_out: str
    encoding: str
    compression: str
    pretty_px: bool
    pretty_ts: bool
    map_symbols: bool
    split_symbols: bool
    split_duration: str
    delivery: str
    max_cost_usd: Decimal
    symbol_source_path: str
    symbol_source_sha256: str
    symbol_count: int

    def __post_init__(self) -> None:
        """Enforce request invariants for every construction path."""
        for name in (
            "dataset",
            "schema",
            "start",
            "end",
            "stype_in",
            "stype_out",
            "encoding",
            "compression",
            "split_duration",
            "delivery",
            "symbol_source_path",
            "symbol_source_sha256",
        ):
            value = getattr(self, name)
            if type(value) is not str or not value:
                raise ValueError(f"{name} must be a non-empty string")

        try:
            start_date = date.fromisoformat(self.start)
            end_date = date.fromisoformat(self.end)
        except ValueError as exc:
            raise ValueError(
                "start and end must be exact ISO dates (YYYY-MM-DD)",
            ) from exc
        if start_date.isoformat() != self.start or end_date.isoformat() != self.end:
            raise ValueError("start and end must be exact ISO dates (YYYY-MM-DD)")
        if start_date >= end_date:
            raise ValueError("exclusive end must be later than start")

        if type(self.symbols) not in {list, tuple} or not self.symbols:
            raise ValueError("symbols must be a non-empty ordered array")
        symbols = tuple(self.symbols)
        if any(type(symbol) is not str or not symbol for symbol in symbols):
            raise ValueError("symbols must contain non-empty strings")
        if len(symbols) > MAX_REQUEST_SYMBOLS:
            raise ValueError(
                f"symbols must contain at most {MAX_REQUEST_SYMBOLS} entries",
            )
        if any(
            len(symbol.encode("utf-8")) > MAX_SYMBOL_UTF8_BYTES for symbol in symbols
        ):
            raise ValueError(
                f"each symbol must be at most {MAX_SYMBOL_UTF8_BYTES} UTF-8 bytes",
            )
        if len(symbols) != len(set(symbols)):
            raise ValueError("symbols must not contain duplicates")

        for name in ("pretty_px", "pretty_ts", "map_symbols", "split_symbols"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")

        enum_values = (
            ("schema", self.schema, self.SCHEMAS),
            ("stype_in", self.stype_in, self.STYPES),
            ("stype_out", self.stype_out, self.STYPES),
            ("encoding", self.encoding, self.ENCODINGS),
            ("compression", self.compression, self.COMPRESSIONS),
            ("split_duration", self.split_duration, self.SPLIT_DURATIONS),
            ("delivery", self.delivery, self.DELIVERIES),
        )
        for name, value, allowed in enum_values:
            if value not in allowed:
                raise ValueError(f"unsupported {name}: {value}")

        if type(self.max_cost_usd) is not Decimal:
            raise ValueError("max_cost_usd must be a Decimal")
        if not self.max_cost_usd.is_finite() or self.max_cost_usd <= 0:
            raise ValueError("max_cost_usd must be finite and greater than zero")
        if self.max_cost_usd > ABSOLUTE_MAX_COST_USD:
            raise ValueError(
                f"max_cost_usd exceeds absolute ceiling {ABSOLUTE_MAX_COST_USD}",
            )

        if len(self.symbol_source_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.symbol_source_sha256
        ):
            raise ValueError("symbol_source_sha256 must be lowercase SHA-256 hex")
        if type(self.symbol_count) is not int:
            raise ValueError("symbol_count must be an integer")
        if self.symbol_count != len(symbols):
            raise ValueError("symbol_count must match the ordered symbols array")

        object.__setattr__(self, "symbols", symbols)

    @classmethod
    def from_toml(cls, path: Path) -> "BatchRequest":
        """Load and validate a request TOML with an exclusive end date."""
        with path.open("rb") as handle:
            raw = tomllib.load(handle)

        _reject_unknown_keys(raw, cls.ROOT_SECTIONS, "TOML", "section")

        request = _require_section(raw, "request")
        guard = _require_section(raw, "guard")
        provenance = _require_section(raw, "provenance")
        _reject_unknown_keys(request, cls.REQUEST_FIELDS, "[request]", "field")
        _reject_unknown_keys(guard, cls.GUARD_FIELDS, "[guard]", "field")
        _reject_unknown_keys(
            provenance,
            cls.PROVENANCE_FIELDS,
            "[provenance]",
            "field",
        )

        max_cost_raw = guard.get("max_cost_usd")
        if type(max_cost_raw) is not str:
            raise ValueError("max_cost_usd must be a decimal string")
        try:
            max_cost_usd = Decimal(max_cost_raw)
        except InvalidOperation as exc:
            raise ValueError("max_cost_usd must be a decimal string") from exc
        return cls(
            dataset=request.get("dataset"),
            schema=request.get("schema"),
            start=request.get("start"),
            end=request.get("end"),
            symbols=request.get("symbols"),
            stype_in=request.get("stype_in"),
            stype_out=request.get("stype_out"),
            encoding=request.get("encoding"),
            compression=request.get("compression"),
            pretty_px=request.get("pretty_px"),
            pretty_ts=request.get("pretty_ts"),
            map_symbols=request.get("map_symbols"),
            split_symbols=request.get("split_symbols"),
            split_duration=request.get("split_duration"),
            delivery=request.get("delivery"),
            max_cost_usd=max_cost_usd,
            symbol_source_path=provenance.get("symbol_source_path"),
            symbol_source_sha256=provenance.get("symbol_source_sha256"),
            symbol_count=provenance.get("symbol_count"),
        )

    def provider_query(self) -> dict[str, object]:
        """Return the exact common query accepted by estimate metadata calls."""
        return {
            "dataset": self.dataset,
            "start": self.start,
            "end": self.end,
            "symbols": list(self.symbols),
            "schema": self.schema,
            "stype_in": self.stype_in,
        }

    def submission(self) -> dict[str, object]:
        """Return the exact argument tuple sent to ``Batch.submit_job``."""
        return {
            **self.provider_query(),
            "stype_out": self.stype_out,
            "encoding": self.encoding,
            "compression": self.compression,
            "pretty_px": self.pretty_px,
            "pretty_ts": self.pretty_ts,
            "map_symbols": self.map_symbols,
            "split_symbols": self.split_symbols,
            "split_duration": self.split_duration,
            "delivery": self.delivery,
        }

    def submission_identity(self) -> str:
        """Hash only the exact provider submission tuple."""
        return hashlib.sha256(_canonical_json(self.submission())).hexdigest()

    def to_dict(self) -> dict[str, object]:
        """Return the complete secret-free request payload."""
        payload = asdict(self)
        payload["symbols"] = list(self.symbols)
        payload["max_cost_usd"] = str(self.max_cost_usd)
        return payload

    def canonical_json(self) -> bytes:
        """Return canonical bytes used by :meth:`fingerprint`."""
        return _canonical_json(self.to_dict())

    def fingerprint(self) -> str:
        """Return SHA-256 over the canonical, complete request payload."""
        return hashlib.sha256(self.canonical_json()).hexdigest()


def _utc_datetime(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return _utc_datetime(value, "timestamp").isoformat().replace("+00:00", "Z")


def _range_timestamp(value: object, name: str) -> datetime:
    """Parse an official range boundary and normalize it to UTC.

    Date-only values retain backward compatibility by representing UTC
    midnight. Timestamp values must carry an explicit timezone.
    """
    if type(value) is not str or not value:
        raise ValueError(f"dataset range {name} must be an ISO timestamp")
    try:
        parsed_date = date.fromisoformat(value)
    except ValueError:
        parsed_date = None
    if parsed_date is not None and parsed_date.isoformat() == value:
        return _DATETIME_PARSER.combine(parsed_date, time.min, tzinfo=timezone.utc)

    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = _DATETIME_PARSER.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"dataset range {name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(
            f"dataset range {name} timestamp must be timezone-aware",
        )
    return parsed.astimezone(timezone.utc)


def _exact_iso_date(value: object, name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} must be an exact ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an exact ISO date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must be an exact ISO date")
    return value


def _deep_freeze(value: object) -> object:
    if type(value) is dict:
        return MappingProxyType(
            {key: _deep_freeze(item) for key, item in value.items()},
        )
    if type(value) in {list, tuple}:
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_thaw(item) for key, item in value.items()}
    if type(value) is tuple:
        return [_deep_thaw(item) for item in value]
    return value


def _validate_provider_query(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise ValueError("provider_query must be an object")
    expected_fields = {
        "dataset",
        "start",
        "end",
        "symbols",
        "schema",
        "stype_in",
    }
    if set(value) != expected_fields:
        raise ValueError(
            "provider_query must contain exactly the provider query fields",
        )
    for name in ("dataset", "schema", "stype_in"):
        if type(value[name]) is not str or not value[name]:
            raise ValueError(f"provider_query.{name} must be a non-empty string")
    start = _exact_iso_date(value["start"], "provider_query.start")
    end = _exact_iso_date(value["end"], "provider_query.end")
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("provider_query exclusive end must be later than start")
    symbols = value["symbols"]
    if (
        type(symbols) is not list
        or not symbols
        or any(type(symbol) is not str or not symbol for symbol in symbols)
    ):
        raise ValueError("provider_query.symbols must be a non-empty string list")
    if len(symbols) != len(set(symbols)):
        raise ValueError("provider_query.symbols must not contain duplicates")
    return value


def _validate_dataset_range(value: object) -> dict[str, object]:
    if type(value) is not dict or set(value) != {"start", "end", "schema"}:
        raise ValueError("dataset_range must contain exactly start, end, schema")
    dataset_start = _range_timestamp(value["start"], "start")
    dataset_end = _range_timestamp(value["end"], "end")
    if dataset_start >= dataset_end:
        raise ValueError("dataset_range end must be later than start")
    schemas = value["schema"]
    if type(schemas) is not dict:
        raise ValueError("dataset_range.schema must be an object")
    for schema, schema_range in schemas.items():
        if type(schema) is not str or not schema:
            raise ValueError("dataset_range.schema keys must be non-empty strings")
        if type(schema_range) is not dict or set(schema_range) != {"start", "end"}:
            raise ValueError(
                f"schema range for {schema!r} must contain exactly start, end",
            )
        schema_start = _range_timestamp(schema_range["start"], "start")
        schema_end = _range_timestamp(schema_range["end"], "end")
        if schema_start >= schema_end:
            raise ValueError(
                f"schema range for {schema!r} end must be later than start"
            )
    return value


def _validate_dataset_conditions(
    value: object,
    start: str,
    end: str,
) -> list[dict[str, object]]:
    if type(value) is not list:
        raise ValueError("dataset_conditions must be an array")
    allowed_fields = {"date", "condition", "last_modified_date"}
    required_fields = {"date", "condition"}
    allowed_conditions = {"available", "degraded", "pending", "missing"}
    condition_dates: list[str] = []
    for item in value:
        if type(item) is not dict:
            raise ValueError("dataset condition entries must be objects")
        if not required_fields.issubset(item) or not set(item).issubset(allowed_fields):
            raise ValueError(
                "dataset condition fields must be exactly date, condition, "
                "and optional last_modified_date",
            )
        condition_dates.append(
            _exact_iso_date(item["date"], "dataset condition date"),
        )
        condition = item["condition"]
        if type(condition) is not str or condition not in allowed_conditions:
            raise ValueError(
                "dataset condition condition must be an official enum value",
            )
        if "last_modified_date" in item and item["last_modified_date"] is not None:
            _exact_iso_date(
                item["last_modified_date"],
                "dataset condition last_modified_date",
            )

    requested_start = date.fromisoformat(start)
    requested_end = date.fromisoformat(end)
    if not condition_dates:
        raise ValueError("dataset conditions must contain at least one provider row")
    if len(condition_dates) != len(set(condition_dates)):
        raise ValueError("dataset condition dates must not contain duplicates")
    if any(
        not requested_start <= date.fromisoformat(item) < requested_end
        for item in condition_dates
    ):
        raise ValueError("dataset condition date is outside the requested range")
    return value


def _derive_availability_reasons(
    provider_query_value: object,
    dataset_range_value: object,
    available_schemas_value: object,
    dataset_conditions_value: object,
) -> tuple[str, ...]:
    """Derive availability only from validated provider metadata and query."""
    provider_query = _validate_provider_query(_deep_thaw(provider_query_value))
    dataset_range = _validate_dataset_range(_deep_thaw(dataset_range_value))
    if type(available_schemas_value) not in {list, tuple}:
        raise ValueError("available_schemas must be an array")
    available_schemas = tuple(available_schemas_value)
    if any(type(schema) is not str or not schema for schema in available_schemas):
        raise ValueError("available_schemas must contain non-empty strings")
    if len(available_schemas) != len(set(available_schemas)):
        raise ValueError("available_schemas must not contain duplicates")
    dataset_conditions = _validate_dataset_conditions(
        _deep_thaw(dataset_conditions_value),
        provider_query["start"],
        provider_query["end"],
    )

    reasons: list[str] = []
    requested_schema = provider_query["schema"]
    if requested_schema not in available_schemas:
        reasons.append(f"schema unavailable: {requested_schema}")

    requested_start = _DATETIME_PARSER.combine(
        date.fromisoformat(provider_query["start"]),
        time.min,
        tzinfo=timezone.utc,
    )
    requested_end = _DATETIME_PARSER.combine(
        date.fromisoformat(provider_query["end"]),
        time.min,
        tzinfo=timezone.utc,
    )

    def check_range(range_value: object, label: str) -> None:
        if type(range_value) is not dict:
            reasons.append(f"{label} range unavailable: expected an object")
            return
        try:
            available_start = _range_timestamp(range_value.get("start"), "start")
            available_end = _range_timestamp(range_value.get("end"), "end")
        except ValueError as exc:
            reasons.append(f"{label} range unavailable: {exc}")
            return
        if requested_start < available_start:
            reasons.append(
                f"{label} range starts after request: "
                f"{_utc_text(available_start)} > {provider_query['start']}T00:00:00Z",
            )
        if requested_end > available_end:
            reasons.append(
                f"{label} range ends before request: "
                f"{_utc_text(available_end)} < {provider_query['end']}T00:00:00Z",
            )

    check_range(dataset_range, "dataset")
    schema_ranges = dataset_range["schema"]
    if type(schema_ranges) is not dict:
        reasons.append("schema range unavailable: expected dataset_range.schema object")
    else:
        check_range(schema_ranges.get(requested_schema), "schema")

    for condition in dataset_conditions:
        condition_name = condition["condition"]
        if condition_name in {"pending", "missing"}:
            reasons.append(
                f"dataset condition {condition_name}: {condition['date']}",
            )
    return tuple(reasons)


@dataclass(frozen=True)
class BatchPlan:
    """Immutable metadata-only estimate used to guard one batch submission."""

    schema_version: str
    request_fingerprint: str
    estimated_at: datetime
    sdk_version: str
    provider_query: Mapping[str, object]
    dataset_range: Mapping[str, object]
    available_schemas: tuple[str, ...]
    dataset_conditions: tuple[Mapping[str, object], ...]
    condition_coverage: str
    estimated_cost_usd: Decimal
    estimated_record_count: int
    estimated_billable_bytes: int
    max_cost_usd: Decimal
    availability_ok: bool
    availability_reasons: tuple[str, ...]
    plan_sha256: str

    def __post_init__(self) -> None:
        if type(self.schema_version) is not str:
            raise ValueError("schema_version must be a string")
        if self.schema_version != PLAN_SCHEMA_VERSION:
            raise ValueError(f"unsupported batch plan schema: {self.schema_version}")
        for name, value in (
            ("request_fingerprint", self.request_fingerprint),
            ("plan_sha256", self.plan_sha256),
        ):
            if (
                type(value) is not str
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{name} must be lowercase SHA-256 hex")
        if type(self.estimated_at) is not _DATETIME_PARSER:
            raise ValueError("estimated_at must be a datetime")
        estimated_at = _utc_datetime(self.estimated_at, "estimated_at")
        if type(self.sdk_version) is not str or not self.sdk_version.strip():
            raise ValueError("sdk_version must be a non-empty string")
        if type(self.estimated_cost_usd) is not Decimal:
            raise ValueError("estimated_cost_usd must be a Decimal")
        if not self.estimated_cost_usd.is_finite() or self.estimated_cost_usd < 0:
            raise ValueError("estimated_cost_usd must be finite and non-negative")
        for name, value in (
            ("estimated_record_count", self.estimated_record_count),
            ("estimated_billable_bytes", self.estimated_billable_bytes),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if type(self.max_cost_usd) is not Decimal:
            raise ValueError("max_cost_usd must be a Decimal")
        if not self.max_cost_usd.is_finite() or self.max_cost_usd <= 0:
            raise ValueError("max_cost_usd must be finite and greater than zero")
        if self.max_cost_usd > ABSOLUTE_MAX_COST_USD:
            raise ValueError(
                f"max_cost_usd exceeds absolute ceiling {ABSOLUTE_MAX_COST_USD}",
            )
        provider_query = _validate_provider_query(_deep_thaw(self.provider_query))
        dataset_range = _validate_dataset_range(_deep_thaw(self.dataset_range))
        dataset_conditions = _validate_dataset_conditions(
            _deep_thaw(self.dataset_conditions),
            provider_query["start"],
            provider_query["end"],
        )
        if type(self.available_schemas) is not tuple:
            raise ValueError("available_schemas must be a tuple")
        available_schemas = tuple(self.available_schemas)
        if any(type(schema) is not str or not schema for schema in available_schemas):
            raise ValueError("available_schemas must contain non-empty strings")
        if len(available_schemas) != len(set(available_schemas)):
            raise ValueError("available_schemas must not contain duplicates")
        if type(self.dataset_conditions) is not tuple:
            raise ValueError("dataset_conditions must be a tuple")
        if type(self.availability_reasons) is not tuple:
            raise ValueError("availability_reasons must be a tuple")
        if any(type(reason) is not str for reason in self.availability_reasons):
            raise ValueError("availability_reasons must contain strings")
        if type(self.availability_ok) is not bool:
            raise ValueError("availability_ok must be a bool")
        if self.condition_coverage != CONDITION_COVERAGE_CLASSIFICATION:
            raise ValueError(
                "condition_coverage must be provider_rows_unverified",
            )
        derived_reasons = _derive_availability_reasons(
            provider_query,
            dataset_range,
            available_schemas,
            dataset_conditions,
        )
        if tuple(
            self.availability_reasons
        ) != derived_reasons or self.availability_ok != (not derived_reasons):
            raise ValueError(
                "plan derived availability does not agree with provider metadata",
            )
        object.__setattr__(self, "provider_query", _deep_freeze(provider_query))
        object.__setattr__(self, "dataset_range", _deep_freeze(dataset_range))
        object.__setattr__(
            self,
            "dataset_conditions",
            _deep_freeze(dataset_conditions),
        )
        object.__setattr__(self, "available_schemas", available_schemas)
        object.__setattr__(self, "estimated_at", estimated_at)
        object.__setattr__(
            self,
            "availability_reasons",
            tuple(self.availability_reasons),
        )

    @classmethod
    def create(
        cls,
        *,
        request: BatchRequest,
        estimated_at: datetime,
        sdk_version: str,
        dataset_range: dict[str, object],
        available_schemas: list[str],
        dataset_conditions: list[dict[str, object]],
        estimated_cost_usd: Decimal,
        estimated_record_count: int,
        estimated_billable_bytes: int,
        availability_reasons: list[str],
    ) -> "BatchPlan":
        plan = cls(
            schema_version=PLAN_SCHEMA_VERSION,
            request_fingerprint=request.fingerprint(),
            estimated_at=_utc_datetime(estimated_at, "estimated_at"),
            sdk_version=sdk_version,
            provider_query=request.provider_query(),
            dataset_range=dataset_range,
            available_schemas=tuple(available_schemas),
            dataset_conditions=tuple(dataset_conditions),
            condition_coverage=CONDITION_COVERAGE_CLASSIFICATION,
            estimated_cost_usd=estimated_cost_usd,
            estimated_record_count=estimated_record_count,
            estimated_billable_bytes=estimated_billable_bytes,
            max_cost_usd=request.max_cost_usd,
            availability_ok=not availability_reasons,
            availability_reasons=tuple(availability_reasons),
            plan_sha256="0" * 64,
        )
        return replace(
            plan,
            plan_sha256=hashlib.sha256(plan.unsigned_canonical_json()).hexdigest(),
        )

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> "BatchPlan":
        """Parse a plan artifact and verify its canonical content hash."""
        expected_fields = {item.name for item in fields(cls)}
        unknown_fields = sorted(set(raw) - expected_fields)
        if unknown_fields:
            raise ValueError(
                f"unknown batch plan fields: {', '.join(unknown_fields)}",
            )
        missing_fields = sorted(expected_fields - set(raw))
        if missing_fields:
            raise ValueError(
                f"missing batch plan fields: {', '.join(missing_fields)}",
            )

        def require_type(name: str, expected_type: type) -> object:
            value = raw[name]
            if type(value) is not expected_type:
                raise ValueError(
                    f"{name} must be {expected_type.__name__}, got {type(value).__name__}",
                )
            return value

        schema_version = require_type("schema_version", str)
        request_fingerprint = require_type("request_fingerprint", str)
        estimated_at_raw = require_type("estimated_at", str)
        sdk_version = require_type("sdk_version", str)
        provider_query = require_type("provider_query", dict)
        dataset_range = require_type("dataset_range", dict)
        available_schemas_raw = require_type("available_schemas", list)
        dataset_conditions_raw = require_type("dataset_conditions", list)
        condition_coverage = require_type("condition_coverage", str)
        estimated_cost_raw = require_type("estimated_cost_usd", str)
        estimated_record_count = require_type("estimated_record_count", int)
        estimated_billable_bytes = require_type("estimated_billable_bytes", int)
        max_cost_raw = require_type("max_cost_usd", str)
        availability_ok = require_type("availability_ok", bool)
        availability_reasons_raw = require_type("availability_reasons", list)
        plan_sha256 = require_type("plan_sha256", str)

        if not sdk_version:
            raise ValueError("sdk_version must be non-empty")
        for name, value in (
            ("request_fingerprint", request_fingerprint),
            ("plan_sha256", plan_sha256),
        ):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValueError(f"{name} must be lowercase SHA-256 hex")

        validated_query = _validate_provider_query(provider_query)
        _validate_dataset_range(dataset_range)
        _validate_dataset_conditions(
            dataset_conditions_raw,
            validated_query["start"],
            validated_query["end"],
        )

        if any(
            type(schema) is not str or not schema for schema in available_schemas_raw
        ):
            raise ValueError("available_schemas must contain non-empty strings")
        if any(type(reason) is not str for reason in availability_reasons_raw):
            raise ValueError("availability_reasons must contain strings")

        try:
            estimated_at = _DATETIME_PARSER.fromisoformat(
                estimated_at_raw.replace("Z", "+00:00")
            )
            estimated_cost_usd = Decimal(estimated_cost_raw)
            max_cost_usd = Decimal(max_cost_raw)
        except (ValueError, InvalidOperation) as exc:
            raise ValueError(f"invalid batch plan value: {exc}") from exc
        if not estimated_cost_usd.is_finite() or estimated_cost_usd < 0:
            raise ValueError("estimated_cost_usd must be finite and non-negative")
        if not max_cost_usd.is_finite() or max_cost_usd <= 0:
            raise ValueError("max_cost_usd must be finite and greater than zero")
        if max_cost_usd > ABSOLUTE_MAX_COST_USD:
            raise ValueError(
                f"max_cost_usd exceeds absolute ceiling {ABSOLUTE_MAX_COST_USD}",
            )
        if estimated_record_count < 0:
            raise ValueError("estimated_record_count must be non-negative")
        if estimated_billable_bytes < 0:
            raise ValueError("estimated_billable_bytes must be non-negative")
        if availability_ok != (not availability_reasons_raw):
            raise ValueError("availability_ok must agree with availability_reasons")

        try:
            plan = cls(
                schema_version=schema_version,
                request_fingerprint=request_fingerprint,
                estimated_at=_utc_datetime(estimated_at, "estimated_at"),
                sdk_version=sdk_version,
                provider_query=provider_query,
                dataset_range=dataset_range,
                available_schemas=tuple(available_schemas_raw),
                dataset_conditions=tuple(dataset_conditions_raw),
                condition_coverage=condition_coverage,
                estimated_cost_usd=estimated_cost_usd,
                estimated_record_count=estimated_record_count,
                estimated_billable_bytes=estimated_billable_bytes,
                max_cost_usd=max_cost_usd,
                availability_ok=availability_ok,
                availability_reasons=tuple(availability_reasons_raw),
                plan_sha256=plan_sha256,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid batch plan: {exc}") from exc
        if plan.schema_version != PLAN_SCHEMA_VERSION:
            raise ValueError(f"unsupported batch plan schema: {plan.schema_version}")
        expected = hashlib.sha256(plan.unsigned_canonical_json()).hexdigest()
        if plan.plan_sha256 != expected:
            raise ValueError("batch plan SHA-256 mismatch")
        return plan

    def unsigned_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "request_fingerprint": self.request_fingerprint,
            "estimated_at": _utc_text(self.estimated_at),
            "sdk_version": self.sdk_version,
            "provider_query": _deep_thaw(self.provider_query),
            "dataset_range": _deep_thaw(self.dataset_range),
            "available_schemas": list(self.available_schemas),
            "dataset_conditions": _deep_thaw(self.dataset_conditions),
            "condition_coverage": self.condition_coverage,
            "estimated_cost_usd": str(self.estimated_cost_usd),
            "estimated_record_count": self.estimated_record_count,
            "estimated_billable_bytes": self.estimated_billable_bytes,
            "max_cost_usd": str(self.max_cost_usd),
            "availability_ok": self.availability_ok,
            "availability_reasons": list(self.availability_reasons),
        }

    def to_dict(self) -> dict[str, object]:
        payload = self.unsigned_dict()
        payload["plan_sha256"] = self.plan_sha256
        return payload

    def unsigned_canonical_json(self) -> bytes:
        return _canonical_json(self.unsigned_dict())

    def canonical_json(self) -> bytes:
        return _canonical_json(self.to_dict())


def load_plan(path: Path) -> BatchPlan:
    """Load and hash-verify a canonical batch plan artifact."""
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("batch plan must be a JSON object")
    return BatchPlan.from_dict(raw)


def _new_client(api_key: str, client_factory: Any) -> tuple[object, str]:
    if client_factory is not None:
        client = client_factory(api_key)
        return client, str(getattr(client, "sdk_version", "unknown"))
    try:
        import databento as db
    except ImportError as exc:
        raise ImportError(
            "databento package not installed. Install with: pip install 'databento-ingest[api]'",
        ) from exc
    client = db.Historical(api_key)
    sdk_version = str(
        getattr(db, "__version__", importlib.metadata.version("databento"))
    )
    return client, sdk_version


def preflight_request(
    api_key: str,
    request: BatchRequest,
    *,
    client_factory: Any = None,
    now: datetime | None = None,
) -> BatchPlan:
    """Collect metadata-only availability, condition, and cost estimates."""
    estimated_at = _utc_datetime(now or datetime.now(timezone.utc), "now")
    client, sdk_version = _new_client(api_key, client_factory)
    metadata = client.metadata  # type: ignore[attr-defined]
    dataset_range = metadata.get_dataset_range(dataset=request.dataset)
    available_schemas = metadata.list_schemas(dataset=request.dataset)
    conditions = metadata.get_dataset_condition(
        dataset=request.dataset,
        start_date=request.start,
        end_date=(date.fromisoformat(request.end) - timedelta(days=1)).isoformat(),
    )
    query = request.provider_query()
    estimated_cost_usd = Decimal(str(metadata.get_cost(**query)))
    estimated_record_count = metadata.get_record_count(**query)
    estimated_billable_bytes = metadata.get_billable_size(**query)
    if not estimated_cost_usd.is_finite() or estimated_cost_usd < 0:
        raise ValueError("estimated cost must be finite and non-negative")
    if not isinstance(estimated_record_count, int) or estimated_record_count < 0:
        raise ValueError("estimated record count must be a non-negative integer")
    if not isinstance(estimated_billable_bytes, int) or estimated_billable_bytes < 0:
        raise ValueError("estimated billable size must be a non-negative integer")
    reasons = list(
        _derive_availability_reasons(
            query,
            dataset_range,
            available_schemas,
            conditions,
        ),
    )
    return BatchPlan.create(
        request=request,
        estimated_at=estimated_at,
        sdk_version=sdk_version,
        dataset_range=dataset_range,
        available_schemas=available_schemas,
        dataset_conditions=conditions,
        estimated_cost_usd=estimated_cost_usd,
        estimated_record_count=estimated_record_count,
        estimated_billable_bytes=estimated_billable_bytes,
        availability_reasons=reasons,
    )


def _sanitize_submit_response(response: object) -> dict[str, object]:
    """Project the provider response onto reviewed safe fields and shapes."""
    if type(response) is not dict:
        raise ValueError("Databento submission response must be an object")
    sanitized: dict[str, object] = {}
    for name in SAFE_SUBMIT_RESPONSE_FIELDS:
        if name not in response:
            continue
        value = response[name]
        if name == "symbols" and type(value) is list:
            if any(type(symbol) not in {str, int} for symbol in value):
                raise ValueError(
                    "Databento submission response field 'symbols' must contain "
                    "only strings or integers",
                )
            sanitized[name] = list(value)
            continue
        if value is not None and type(value) not in {str, int, float, bool}:
            raise ValueError(
                f"Databento submission response field {name!r} must be scalar",
            )
        if type(value) is float and not math.isfinite(value):
            raise ValueError(
                f"Databento submission response field {name!r} must be finite",
            )
        sanitized[name] = value
    return sanitized


def _submission_state_root() -> Path:
    """Return the fixed per-user ledger root, unless explicitly overridden."""
    override = os.environ.get("DATABENTO_INGEST_STATE_DIR")
    if override:
        override_path = Path(override).expanduser()
        if not override_path.is_absolute():
            raise ValueError(
                "DATABENTO_INGEST_STATE_DIR must be an absolute path",
            )
        return override_path
    return Path.home() / ".local/state/databento-ingest"


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )


def _record_open_flags(flags: int) -> int:
    return flags | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)


def _open_directory_path(
    path: Path,
    *,
    create_final: bool,
    create_default_parents: bool,
) -> int:
    """Open an absolute directory path one no-follow component at a time."""
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise AcquisitionRefused(
            "submission ledger directory must be an absolute canonical path",
        )
    components = path.parts[1:]
    current_fd = os.open("/", _directory_open_flags())
    try:
        for index, component in enumerate(components):
            is_final = index == len(components) - 1
            try:
                next_fd = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=current_fd,
                )
            except FileNotFoundError:
                if not (is_final and create_final) and not create_default_parents:
                    raise AcquisitionRefused(
                        f"submission ledger directory does not exist: {path}",
                    ) from None
                try:
                    os.mkdir(component, 0o700, dir_fd=current_fd)
                except FileExistsError:
                    pass
                try:
                    next_fd = os.open(
                        component,
                        _directory_open_flags(),
                        dir_fd=current_fd,
                    )
                except OSError as exc:
                    raise AcquisitionRefused(
                        "submission ledger directory component is not a secure "
                        "directory",
                    ) from exc
            except OSError as exc:
                raise AcquisitionRefused(
                    "submission ledger directory component is a symlink or not "
                    "a directory",
                ) from exc
            os.close(current_fd)
            current_fd = next_fd
        return current_fd
    except BaseException:
        os.close(current_fd)
        raise


def _validate_ledger_directory(fd: int, label: str) -> tuple[int, int]:
    status = os.fstat(fd)
    if not stat.S_ISDIR(status.st_mode):
        raise AcquisitionRefused(f"submission ledger {label} is not a directory")
    if status.st_uid != os.geteuid():
        raise AcquisitionRefused(
            f"submission ledger {label} must be owned by the current user",
        )
    mode = stat.S_IMODE(status.st_mode)
    if mode != 0o700:
        raise AcquisitionRefused(
            f"submission ledger {label} mode must be exactly 0700, got {mode:04o}",
        )
    return status.st_dev, status.st_ino


@dataclass
class _SubmissionLedger:
    root_path: Path
    root_fd: int
    submissions_fd: int
    root_identity: tuple[int, int]
    submissions_identity: tuple[int, int]
    lock_name: str
    lock_fd: int
    lock_identity: tuple[int, int]
    record_name: str
    record_fd: int | None = None
    record_identity: tuple[int, int] | None = None
    closed: bool = False

    def close(self) -> None:
        if self.closed:
            return
        if self.record_fd is not None:
            os.close(self.record_fd)
        os.close(self.lock_fd)
        os.close(self.submissions_fd)
        os.close(self.root_fd)
        self.closed = True


def _open_submission_ledger(submission_identity: str) -> _SubmissionLedger:
    if len(submission_identity) != 64 or any(
        character not in "0123456789abcdef" for character in submission_identity
    ):
        raise AcquisitionRefused("submission identity must be lowercase SHA-256 hex")
    root_path = _submission_state_root()
    using_override = bool(os.environ.get("DATABENTO_INGEST_STATE_DIR"))
    root_fd = _open_directory_path(
        root_path,
        create_final=True,
        create_default_parents=not using_override,
    )
    submissions_fd: int | None = None
    lock_fd: int | None = None
    try:
        root_identity = _validate_ledger_directory(root_fd, "root")
        try:
            os.mkdir("submissions", 0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        try:
            submissions_fd = os.open(
                "submissions",
                _directory_open_flags(),
                dir_fd=root_fd,
            )
        except OSError as exc:
            raise AcquisitionRefused(
                "submission ledger submissions component is a symlink or not a "
                "directory",
            ) from exc
        submissions_identity = _validate_ledger_directory(
            submissions_fd,
            "submissions directory",
        )
        lock_name = f"{submission_identity}.lock"
        try:
            lock_fd = os.open(
                lock_name,
                _record_open_flags(os.O_RDWR | os.O_CREAT),
                0o600,
                dir_fd=submissions_fd,
            )
        except OSError as exc:
            raise AcquisitionRefused(
                "submission identity lock is a symlink or inaccessible",
            ) from exc
        lock_identity = _validate_ledger_lock_fd(lock_fd)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            raise AcquisitionRefused(
                f"submission identity is locked by another writer: {submission_identity}",
            ) from exc
        return _SubmissionLedger(
            root_path=root_path,
            root_fd=root_fd,
            submissions_fd=submissions_fd,
            root_identity=root_identity,
            submissions_identity=submissions_identity,
            lock_name=lock_name,
            lock_fd=lock_fd,
            lock_identity=lock_identity,
            record_name=f"{submission_identity}.json",
        )
    except BaseException:
        if lock_fd is not None:
            os.close(lock_fd)
        if submissions_fd is not None:
            os.close(submissions_fd)
        os.close(root_fd)
        raise


def _assert_ledger_binding(ledger: _SubmissionLedger) -> None:
    """Require held descriptors and the selected path to name the same roots."""
    if ledger.closed:
        raise AcquisitionRefused("submission ledger binding is closed")
    if _validate_ledger_directory(ledger.root_fd, "root") != ledger.root_identity:
        raise AcquisitionRefused("submission ledger root binding changed")
    if (
        _validate_ledger_directory(
            ledger.submissions_fd,
            "submissions directory",
        )
        != ledger.submissions_identity
    ):
        raise AcquisitionRefused("submission ledger submissions binding changed")
    try:
        held_lock_identity = _validate_ledger_lock_fd(ledger.lock_fd)
    except AcquisitionRefused as exc:
        raise AcquisitionRefused("submission ledger lock binding changed") from exc
    if held_lock_identity != ledger.lock_identity:
        raise AcquisitionRefused("submission ledger lock binding changed")
    try:
        lock_status = os.stat(
            ledger.lock_name,
            dir_fd=ledger.submissions_fd,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise AcquisitionRefused("submission ledger lock binding changed") from exc
    if (lock_status.st_dev, lock_status.st_ino) != ledger.lock_identity:
        raise AcquisitionRefused("submission ledger lock binding changed")
    if ledger.record_fd is not None:
        if ledger.record_identity is None:
            raise AcquisitionRefused("submission ledger record binding is incomplete")
        try:
            held_record_identity = _validate_ledger_record_fd(ledger.record_fd)
        except AcquisitionRefused as exc:
            raise AcquisitionRefused(
                "submission ledger record binding changed",
            ) from exc
        if held_record_identity != ledger.record_identity:
            raise AcquisitionRefused("submission ledger record binding changed")
        try:
            record_status = os.stat(
                ledger.record_name,
                dir_fd=ledger.submissions_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise AcquisitionRefused(
                "submission ledger record binding changed"
            ) from exc
        if (record_status.st_dev, record_status.st_ino) != ledger.record_identity:
            raise AcquisitionRefused("submission ledger record binding changed")

    reopened_root_fd = _open_directory_path(
        ledger.root_path,
        create_final=False,
        create_default_parents=False,
    )
    reopened_submissions_fd: int | None = None
    try:
        if _validate_ledger_directory(reopened_root_fd, "root") != ledger.root_identity:
            raise AcquisitionRefused("submission ledger root binding changed")
        try:
            reopened_submissions_fd = os.open(
                "submissions",
                _directory_open_flags(),
                dir_fd=reopened_root_fd,
            )
        except OSError as exc:
            raise AcquisitionRefused(
                "submission ledger submissions binding changed",
            ) from exc
        if (
            _validate_ledger_directory(
                reopened_submissions_fd,
                "submissions directory",
            )
            != ledger.submissions_identity
        ):
            raise AcquisitionRefused("submission ledger submissions binding changed")
    finally:
        if reopened_submissions_fd is not None:
            os.close(reopened_submissions_fd)
        os.close(reopened_root_fd)


def _validate_ledger_record_fd(fd: int) -> tuple[int, int]:
    status = os.fstat(fd)
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
        raise AcquisitionRefused(
            "submission ledger record must be a singly linked regular file",
        )
    if status.st_uid != os.geteuid():
        raise AcquisitionRefused(
            "submission ledger record must be owned by the current user",
        )
    mode = stat.S_IMODE(status.st_mode)
    if mode != 0o600:
        raise AcquisitionRefused(
            f"submission ledger record mode must be exactly 0600, got {mode:04o}",
        )
    if status.st_size > MAX_SUBMISSION_JOURNAL_BYTES:
        raise AcquisitionRefused("submission ledger record is too large")
    return status.st_dev, status.st_ino


def _validate_ledger_lock_fd(fd: int) -> tuple[int, int]:
    status = os.fstat(fd)
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
        raise AcquisitionRefused(
            "submission ledger lock must be a singly linked regular file",
        )
    if status.st_uid != os.geteuid():
        raise AcquisitionRefused(
            "submission ledger lock must be owned by the current user",
        )
    mode = stat.S_IMODE(status.st_mode)
    if mode != 0o600:
        raise AcquisitionRefused(
            f"submission ledger lock mode must be exactly 0600, got {mode:04o}",
        )
    if status.st_size != 0:
        raise AcquisitionRefused("submission ledger lock must be empty")
    return status.st_dev, status.st_ino


def _state_record_sha256(record: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json(dict(record))).hexdigest()


def _utf8_size(value: str) -> int:
    return len(value.encode("utf-8"))


def _bounded_state_reason(reason: str, fallback: str) -> str:
    """Keep diagnostic state text bounded without silently truncating UTF-8."""
    if reason and _utf8_size(reason) <= MAX_STATE_REASON_UTF8_BYTES:
        return reason
    digest = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:16]
    return f"{fallback}:{digest}"


def _validate_state_timestamp(record: Mapping[str, object], name: str) -> None:
    value = record.get(name)
    if type(value) is not str:
        raise AcquisitionRefused(
            f"submission ledger {name} must be a canonical UTC timestamp",
        )
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = _DATETIME_PARSER.fromisoformat(normalized)
    except ValueError as exc:
        raise AcquisitionRefused(
            f"submission ledger {name} must be a canonical UTC timestamp",
        ) from exc
    try:
        canonical = _utc_text(parsed)
    except ValueError as exc:
        raise AcquisitionRefused(
            f"submission ledger {name} must be a canonical UTC timestamp",
        ) from exc
    if canonical != value:
        raise AcquisitionRefused(
            f"submission ledger {name} must be a canonical UTC timestamp",
        )


def _validate_submission_journal(
    records: list[dict[str, object]],
    submission_identity: str,
) -> None:
    if not records:
        raise AcquisitionRefused("submission ledger journal is empty")
    expected_submission: object | None = None
    previous: dict[str, object] | None = None
    timestamp_fields = {
        "reserved": ("reserved_at",),
        "released": ("reserved_at", "released_at"),
        "attempted": ("reserved_at", "attempted_at"),
        "aborted": ("reserved_at", "attempted_at", "aborted_at"),
        "consumed": ("reserved_at", "attempted_at", "consumed_at"),
    }
    allowed_transitions = {
        "reserved": frozenset({"attempted", "released"}),
        "released": frozenset({"reserved"}),
        "attempted": frozenset({"aborted", "consumed"}),
        "aborted": frozenset(),
        "consumed": frozenset(),
    }
    state_fields = {
        "reserved": frozenset(),
        "released": frozenset({"released_at", "release_reason"}),
        "attempted": frozenset({"attempted_at"}),
        "aborted": frozenset({"attempted_at", "aborted_at", "abort_reason"}),
        "consumed": frozenset({"attempted_at", "consumed_at", "job_id"}),
    }
    common_fields = {
        "schema_version",
        "submission_identity_sha256",
        "submission",
        "state",
        "reserved_at",
        "attempt",
        "state_sequence",
        "previous_state_sha256",
    }
    for index, record in enumerate(records, start=1):
        state = record.get("state")
        if type(state) is not str or state not in state_fields:
            raise AcquisitionRefused("submission ledger state is invalid")
        if set(record) != common_fields | state_fields[state]:
            raise AcquisitionRefused("submission ledger state fields are invalid")
        if record.get("schema_version") != SUBMISSION_STATE_SCHEMA_VERSION:
            raise AcquisitionRefused("submission ledger schema version is invalid")
        if record.get("submission_identity_sha256") != submission_identity:
            raise AcquisitionRefused("submission ledger identity is invalid")
        submission = record.get("submission")
        if type(submission) is not dict:
            raise AcquisitionRefused("submission ledger provider tuple is invalid")
        if (
            hashlib.sha256(_canonical_json(submission)).hexdigest()
            != submission_identity
        ):
            raise AcquisitionRefused("submission ledger provider tuple is invalid")
        if expected_submission is None:
            expected_submission = submission
        elif submission != expected_submission:
            raise AcquisitionRefused("submission ledger provider tuple changed")
        if (
            type(record.get("state_sequence")) is not int
            or record["state_sequence"] != index
        ):
            raise AcquisitionRefused("submission ledger state sequence is invalid")
        attempt = record.get("attempt")
        if type(attempt) is not int or attempt <= 0:
            raise AcquisitionRefused("submission ledger attempt is invalid")
        for timestamp_field in timestamp_fields[state]:
            _validate_state_timestamp(record, timestamp_field)
        if state == "released" and (
            type(record.get("release_reason")) is not str
            or not record["release_reason"]
            or _utf8_size(record["release_reason"]) > MAX_STATE_REASON_UTF8_BYTES
        ):
            raise AcquisitionRefused("submission ledger release reason is invalid")
        if state == "aborted" and (
            type(record.get("abort_reason")) is not str
            or not record["abort_reason"]
            or _utf8_size(record["abort_reason"]) > MAX_STATE_REASON_UTF8_BYTES
        ):
            raise AcquisitionRefused("submission ledger abort reason is invalid")
        if state == "consumed" and (
            type(record.get("job_id")) is not str
            or not record["job_id"]
            or _utf8_size(record["job_id"]) > MAX_JOB_ID_UTF8_BYTES
        ):
            raise AcquisitionRefused("submission ledger job ID is invalid")
        if previous is None:
            if state != "reserved" or attempt != 1:
                raise AcquisitionRefused("submission ledger must begin reserved")
            if record.get("previous_state_sha256") is not None:
                raise AcquisitionRefused("submission ledger first link is invalid")
        else:
            previous_state = previous["state"]
            if state not in allowed_transitions[previous_state]:
                raise AcquisitionRefused(
                    "submission ledger state transition is invalid"
                )
            if record.get("previous_state_sha256") != _state_record_sha256(previous):
                raise AcquisitionRefused("submission ledger hash chain is invalid")
            previous_attempt = previous["attempt"]
            expected_attempt = (
                previous_attempt + 1
                if previous_state == "released" and state == "reserved"
                else previous_attempt
            )
            if attempt != expected_attempt:
                raise AcquisitionRefused(
                    "submission ledger attempt sequence is invalid"
                )
        previous = record


def _read_submission_journal(ledger: _SubmissionLedger) -> list[dict[str, object]]:
    if ledger.record_fd is None or ledger.record_identity is None:
        raise AcquisitionRefused("submission ledger record is not open")
    _assert_ledger_binding(ledger)
    size = os.fstat(ledger.record_fd).st_size
    if size <= 0 or size > MAX_SUBMISSION_JOURNAL_BYTES:
        raise AcquisitionRefused("submission ledger record is corrupt")
    payload = os.pread(ledger.record_fd, size + 1, 0)
    if len(payload) != size or not payload.endswith(b"\n"):
        raise AcquisitionRefused("submission ledger record is corrupt")
    records: list[dict[str, object]] = []
    try:
        for line in payload.splitlines(keepends=True):
            raw = json.loads(line.decode("utf-8"))
            if type(raw) is not dict or line != _canonical_json(raw) + b"\n":
                raise AcquisitionRefused("submission ledger record is corrupt")
            records.append(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise AcquisitionRefused("submission ledger record is corrupt") from exc
    submission_identity = ledger.record_name.removesuffix(".json")
    _validate_submission_journal(records, submission_identity)
    return records


def _read_submission_record(ledger: _SubmissionLedger) -> dict[str, object]:
    return _read_submission_journal(ledger)[-1]


def _linked_state_record(
    expected: Mapping[str, object],
    state: str,
    **state_values: object,
) -> dict[str, object]:
    """Build one exact successor without carrying prior state-only fields."""
    record = {
        "schema_version": expected["schema_version"],
        "submission_identity_sha256": expected["submission_identity_sha256"],
        "submission": expected["submission"],
        "state": state,
        "reserved_at": expected["reserved_at"],
        "attempt": expected["attempt"],
        "state_sequence": int(expected["state_sequence"]) + 1,
        "previous_state_sha256": _state_record_sha256(expected),
    }
    record.update(state_values)
    return record


def _record_payload_size(record: Mapping[str, object]) -> int:
    return len(_canonical_json(dict(record))) + 1


def _require_journal_capacity(current_size: int, additional_size: int) -> None:
    if (
        current_size < 0
        or additional_size < 0
        or current_size + additional_size > MAX_SUBMISSION_JOURNAL_BYTES
    ):
        raise AcquisitionRefused(_CAPACITY_EXHAUSTED_MESSAGE)


def _terminal_capacity_size(attempted_record: Mapping[str, object]) -> int:
    aborted = _linked_state_record(
        attempted_record,
        "aborted",
        attempted_at=attempted_record["attempted_at"],
        aborted_at=_MAX_CANONICAL_UTC_TEXT,
        abort_reason="\x00" * MAX_STATE_REASON_UTF8_BYTES,
    )
    consumed = _linked_state_record(
        attempted_record,
        "consumed",
        attempted_at=attempted_record["attempted_at"],
        consumed_at=_MAX_CANONICAL_UTC_TEXT,
        job_id="\x00" * MAX_JOB_ID_UTF8_BYTES,
    )
    return max(_record_payload_size(aborted), _record_payload_size(consumed))


def _reservation_path_capacity_size(reserved_record: Mapping[str, object]) -> int:
    released = _linked_state_record(
        reserved_record,
        "released",
        released_at=_MAX_CANONICAL_UTC_TEXT,
        release_reason="\x00" * MAX_STATE_REASON_UTF8_BYTES,
    )
    attempted = _linked_state_record(
        reserved_record,
        "attempted",
        attempted_at=_MAX_CANONICAL_UTC_TEXT,
    )
    return _record_payload_size(reserved_record) + max(
        _record_payload_size(released),
        _record_payload_size(attempted) + _terminal_capacity_size(attempted),
    )


def _require_attempt_capacity(
    ledger: _SubmissionLedger,
    reserved_record: Mapping[str, object],
    attempted_record: Mapping[str, object],
) -> None:
    if ledger.record_fd is None:
        raise AcquisitionRefused("submission ledger record is not open")
    released = _linked_state_record(
        reserved_record,
        "released",
        released_at=_MAX_CANONICAL_UTC_TEXT,
        release_reason="\x00" * MAX_STATE_REASON_UTF8_BYTES,
    )
    additional_size = max(
        _record_payload_size(released),
        _record_payload_size(attempted_record)
        + _terminal_capacity_size(attempted_record),
    )
    _require_journal_capacity(os.fstat(ledger.record_fd).st_size, additional_size)


def _append_record_payload(fd: int, record: dict[str, object]) -> None:
    _validate_ledger_record_fd(fd)
    payload = _canonical_json(record) + b"\n"
    _require_journal_capacity(os.fstat(fd).st_size, len(payload))
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("submission ledger append made no progress")
        view = view[written:]
    os.fsync(fd)


def _reserve_submission_identity(
    submission_identity: str,
    submission: dict[str, object],
    reserved_at: datetime,
) -> tuple[_SubmissionLedger, dict[str, object]]:
    """Reserve one provider tuple while holding its descriptor-anchored lock."""
    ledger = _open_submission_ledger(submission_identity)
    record: dict[str, object] = {
        "schema_version": SUBMISSION_STATE_SCHEMA_VERSION,
        "submission_identity_sha256": submission_identity,
        "submission": submission,
        "state": "reserved",
        "reserved_at": _utc_text(reserved_at),
        "attempt": 1,
        "state_sequence": 1,
        "previous_state_sha256": None,
    }
    try:
        _require_journal_capacity(0, _reservation_path_capacity_size(record))
        try:
            record_fd = os.open(
                ledger.record_name,
                _record_open_flags(
                    os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL,
                ),
                0o600,
                dir_fd=ledger.submissions_fd,
            )
        except FileExistsError:
            try:
                record_fd = os.open(
                    ledger.record_name,
                    _record_open_flags(os.O_RDWR | os.O_APPEND),
                    dir_fd=ledger.submissions_fd,
                )
            except OSError as exc:
                raise AcquisitionRefused(
                    "submission ledger record is a symlink or inaccessible",
                ) from exc
            ledger.record_fd = record_fd
            ledger.record_identity = _validate_ledger_record_fd(record_fd)
            current = _read_submission_record(ledger)
            if current.get("submission") != submission:
                raise AcquisitionRefused(
                    "submission identity already reserved or consumed: "
                    f"{submission_identity}",
                )
            if current.get("state") == "reserved":
                recovered = _linked_state_record(
                    current,
                    "released",
                    released_at=_utc_text(reserved_at),
                    release_reason="recovered_orphaned_reservation",
                )
                record = _linked_state_record(
                    recovered,
                    "reserved",
                    reserved_at=_utc_text(reserved_at),
                    attempt=int(current["attempt"]) + 1,
                )
                current_size = os.fstat(record_fd).st_size
                _require_journal_capacity(
                    current_size,
                    _record_payload_size(recovered)
                    + _reservation_path_capacity_size(record),
                )
                _replace_submission_state(ledger, current, recovered)
                current = recovered
            elif current.get("state") != "released":
                raise AcquisitionRefused(
                    "submission identity already reserved or consumed: "
                    f"{submission_identity}",
                )
            record = _linked_state_record(
                current,
                "reserved",
                reserved_at=_utc_text(reserved_at),
                attempt=int(current["attempt"]) + 1,
            )
            _require_journal_capacity(
                os.fstat(record_fd).st_size,
                _reservation_path_capacity_size(record),
            )
            _replace_submission_state(ledger, current, record)
            return ledger, record

        ledger.record_fd = record_fd
        ledger.record_identity = _validate_ledger_record_fd(record_fd)
        _append_record_payload(record_fd, record)
        os.fsync(ledger.submissions_fd)
        _assert_ledger_binding(ledger)
        if _read_submission_record(ledger) != record:
            raise AcquisitionRefused("submission ledger reservation is corrupt")
    except BaseException:
        ledger.close()
        raise
    return ledger, record


def _replace_submission_state(
    ledger: _SubmissionLedger,
    expected_record: dict[str, object],
    replacement_record: dict[str, object],
) -> None:
    """Append a hash-chained state without replacing any filesystem pathname."""
    _assert_ledger_binding(ledger)
    if _read_submission_record(ledger) != expected_record:
        raise AcquisitionRefused("submission ledger record changed unexpectedly")
    if _read_submission_record(ledger) != expected_record:
        raise AcquisitionRefused("submission ledger record changed unexpectedly")
    replacement_record["state_sequence"] = expected_record["state_sequence"] + 1
    replacement_record["previous_state_sha256"] = _state_record_sha256(
        expected_record,
    )
    journal = _read_submission_journal(ledger)
    if journal[-1] != expected_record:
        raise AcquisitionRefused("submission ledger record changed unexpectedly")
    _validate_submission_journal(
        [*journal, replacement_record],
        expected_record["submission_identity_sha256"],
    )
    _assert_ledger_binding(ledger)
    if ledger.record_fd is None:
        raise AcquisitionRefused("submission ledger record is not open")
    _append_record_payload(ledger.record_fd, replacement_record)
    _assert_ledger_binding(ledger)
    if _read_submission_record(ledger) != replacement_record:
        raise AcquisitionRefused("submission ledger append is corrupt")


def _release_reserved_submission(
    ledger: _SubmissionLedger,
    reserved_record: dict[str, object],
    released_at: datetime,
    release_reason: str,
) -> None:
    """Append a retryable release without unlinking any filesystem pathname."""
    try:
        _assert_ledger_binding(ledger)
        raw = _read_submission_record(ledger)
    except (OSError, AcquisitionRefused):
        return
    if raw != reserved_record or raw.get("state") != "reserved":
        return
    try:
        released_record = {
            **reserved_record,
            "state": "released",
            "released_at": _utc_text(released_at),
            "release_reason": _bounded_state_reason(
                release_reason,
                "pre_attempt_error",
            ),
        }
        _replace_submission_state(ledger, reserved_record, released_record)
    except (OSError, AcquisitionRefused):
        return


def _sample_submission_clock(clock: Callable[[], datetime] | None) -> datetime:
    sampled = datetime.now(timezone.utc) if clock is None else clock()
    if not isinstance(sampled, _DATETIME_PARSER):
        raise TypeError("submission clock must return a datetime")
    return _utc_datetime(sampled, "submission clock")


def submit_planned_request(
    api_key: str,
    request: BatchRequest,
    plan: BatchPlan,
    *,
    client_factory: Any = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Submit with local at-most-once protection in one intact selected root.

    The state-root override selects a separate local idempotency domain. This
    local cooperative-writer filesystem protocol is not cross-root or provider
    exactly-once delivery.
    """
    if clock is not None and not callable(clock):
        raise TypeError("submission clock must be callable")
    if (
        request.max_cost_usd > ABSOLUTE_MAX_COST_USD
        or plan.max_cost_usd > ABSOLUTE_MAX_COST_USD
    ):
        raise AcquisitionRefused(
            f"request or plan exceeds absolute cost ceiling {ABSOLUTE_MAX_COST_USD}",
        )
    checked_at = _sample_submission_clock(clock)
    expected_plan_hash = hashlib.sha256(plan.unsigned_canonical_json()).hexdigest()
    if plan.plan_sha256 != expected_plan_hash:
        raise AcquisitionRefused("plan SHA-256 mismatch")
    if plan.request_fingerprint != request.fingerprint():
        raise AcquisitionRefused("request fingerprint differs from plan")
    if _deep_thaw(plan.provider_query) != request.provider_query():
        raise AcquisitionRefused("plan provider query differs from request")
    try:
        derived_availability_reasons = _derive_availability_reasons(
            request.provider_query(),
            plan.dataset_range,
            plan.available_schemas,
            plan.dataset_conditions,
        )
    except ValueError as exc:
        raise AcquisitionRefused(
            f"plan derived availability metadata is invalid: {exc}",
        ) from exc
    if (
        plan.availability_reasons != derived_availability_reasons
        or plan.availability_ok != (not derived_availability_reasons)
    ):
        raise AcquisitionRefused(
            "plan derived availability does not agree with provider metadata",
        )
    if plan.max_cost_usd != request.max_cost_usd:
        raise AcquisitionRefused("plan cost cap differs from request")
    age = checked_at - plan.estimated_at
    if age < timedelta(0):
        raise AcquisitionRefused("plan estimate timestamp is in the future")
    if age > PLAN_MAX_AGE:
        raise AcquisitionRefused("plan is older than 15 minutes")
    if plan.estimated_cost_usd > request.max_cost_usd:
        raise AcquisitionRefused("plan estimate exceeds configured cost cap")
    if not plan.availability_ok:
        raise AcquisitionRefused("plan availability check failed")

    submission = request.submission()
    submission_identity = request.submission_identity()
    ledger, reserved_record = _reserve_submission_identity(
        submission_identity,
        submission,
        checked_at,
    )
    state_record = reserved_record
    release_time = checked_at
    try:
        try:
            client, sdk_version = _new_client(api_key, client_factory)
            query = request.provider_query()
            requoted_cost_usd = Decimal(  # type: ignore[attr-defined]
                str(client.metadata.get_cost(**query)),
            )
            if not requoted_cost_usd.is_finite() or requoted_cost_usd < 0:
                raise AcquisitionRefused("immediate re-quote is invalid")
            if requoted_cost_usd > request.max_cost_usd:
                raise AcquisitionRefused(
                    "immediate re-quote exceeds configured cost cap",
                )

            attempted_at = _sample_submission_clock(clock)
            release_time = attempted_at
            age = attempted_at - plan.estimated_at
            if age < timedelta(0):
                raise AcquisitionRefused("plan estimate timestamp is in the future")
            if age > PLAN_MAX_AGE:
                raise AcquisitionRefused("plan is older than 15 minutes")
            if (
                request.max_cost_usd > ABSOLUTE_MAX_COST_USD
                or plan.max_cost_usd > ABSOLUTE_MAX_COST_USD
            ):
                raise AcquisitionRefused(
                    "request or plan exceeds absolute cost ceiling "
                    f"{ABSOLUTE_MAX_COST_USD}",
                )
            attempted_record = _linked_state_record(
                state_record,
                "attempted",
                attempted_at=_utc_text(attempted_at),
            )
            _require_attempt_capacity(ledger, state_record, attempted_record)
            _replace_submission_state(ledger, state_record, attempted_record)
            state_record = attempted_record
        except BaseException as exc:
            _release_reserved_submission(
                ledger,
                reserved_record,
                release_time,
                type(exc).__name__,
            )
            raise

        _assert_ledger_binding(ledger)
        submitted_at = _sample_submission_clock(clock)
        final_age = submitted_at - plan.estimated_at
        abort_reason: str | None = None
        if final_age < timedelta(0):
            abort_reason = "plan estimate timestamp is in the future"
        elif final_age > PLAN_MAX_AGE:
            abort_reason = "plan is older than 15 minutes"
        elif (
            request.max_cost_usd > ABSOLUTE_MAX_COST_USD
            or plan.max_cost_usd > ABSOLUTE_MAX_COST_USD
        ):
            abort_reason = (
                f"request or plan exceeds absolute cost ceiling {ABSOLUTE_MAX_COST_USD}"
            )
        if abort_reason is not None:
            aborted_record = _linked_state_record(
                state_record,
                "aborted",
                attempted_at=state_record["attempted_at"],
                aborted_at=_utc_text(submitted_at),
                abort_reason=_bounded_state_reason(
                    abort_reason,
                    "post_attempt_abort",
                ),
            )
            _replace_submission_state(ledger, state_record, aborted_record)
            state_record = aborted_record
            raise AcquisitionRefused(abort_reason)

        raw_response = client.batch.submit_job(  # type: ignore[attr-defined]
            **submission,
        )
        response = _sanitize_submit_response(raw_response)
        job_id = response.get("id", response.get("job_id"))
        if type(job_id) is not str or not job_id:
            raise ValueError("Databento submission response is missing a job ID")
        if _utf8_size(job_id) > MAX_JOB_ID_UTF8_BYTES:
            raise ValueError(
                "Databento submission response job ID exceeds "
                f"{MAX_JOB_ID_UTF8_BYTES} UTF-8 bytes",
            )
        consumed_at = _sample_submission_clock(clock)
        consumed_record = _linked_state_record(
            state_record,
            "consumed",
            attempted_at=state_record["attempted_at"],
            consumed_at=_utc_text(consumed_at),
            job_id=job_id,
        )
        _replace_submission_state(ledger, state_record, consumed_record)
        state_record = consumed_record
        receipt: dict[str, object] = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "request_fingerprint": request.fingerprint(),
            "submission_identity_sha256": submission_identity,
            "plan_sha256": plan.plan_sha256,
            "submitted_at": _utc_text(submitted_at),
            "sdk_version": sdk_version,
            "estimated_cost_usd": str(plan.estimated_cost_usd),
            "requoted_cost_usd": str(requoted_cost_usd),
            "max_cost_usd": str(request.max_cost_usd),
            "job_id": job_id,
            "provider_response": response,
        }
        receipt["receipt_sha256"] = hashlib.sha256(
            _canonical_json(receipt),
        ).hexdigest()
        return receipt
    finally:
        ledger.close()
