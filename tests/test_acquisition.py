"""Contract tests for typed, cost-capped Databento batch acquisition."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import hashlib
import importlib.metadata
import inspect
import json
import argparse
import multiprocessing
import os
import stat
import tomllib
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import databento_ingest.acquisition as acquisition_module
from databento_ingest.acquisition import (
    AcquisitionRefused,
    BatchPlan,
    BatchRequest,
    load_plan,
    preflight_request,
    submit_planned_request,
    write_canonical_json_no_clobber,
)
from databento_ingest import batch, cli


SOURCE_SHA256 = "a" * 64


@pytest.fixture(autouse=True)
def _isolate_submission_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DATABENTO_INGEST_STATE_DIR",
        str(tmp_path / "submission-state"),
    )


def _write_request(
    path: Path,
    *,
    symbols: tuple[str, ...] = ("NVDA", "AMD"),
    start: str = "2025-02-03",
    end: str = "2026-07-10",
    encoding: str = "dbn",
    compression: str = "zstd",
    split_duration: str = "day",
    split_symbols: bool = False,
    max_cost_usd: str = "1.00",
) -> Path:
    symbols_toml = ", ".join(json.dumps(symbol) for symbol in symbols)
    path.write_text(
        f"""
[request]
dataset = "XNAS.ITCH"
schema = "statistics"
start = "{start}"
end = "{end}"
symbols = [{symbols_toml}]
stype_in = "raw_symbol"
stype_out = "instrument_id"
encoding = "{encoding}"
compression = "{compression}"
pretty_px = false
pretty_ts = false
map_symbols = false
split_symbols = {str(split_symbols).lower()}
split_duration = "{split_duration}"
delivery = "download"

[guard]
max_cost_usd = "{max_cost_usd}"

[provenance]
symbol_source_path = "/sealed/provider/metadata.json"
symbol_source_sha256 = "{SOURCE_SHA256}"
symbol_count = {len(symbols)}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def test_request_preserves_exact_exclusive_range_and_symbol_order(
    tmp_path: Path,
) -> None:
    request = BatchRequest.from_toml(_write_request(tmp_path / "request.toml"))

    assert request.start == "2025-02-03"
    assert request.end == "2026-07-10"
    assert request.symbols == ("NVDA", "AMD")
    assert request.provider_query() == {
        "dataset": "XNAS.ITCH",
        "start": "2025-02-03",
        "end": "2026-07-10",
        "symbols": ["NVDA", "AMD"],
        "schema": "statistics",
        "stype_in": "raw_symbol",
    }
    assert request.max_cost_usd == Decimal("1.00")


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2025-02-03T00:00:00Z", "2026-07-10"),
        ("2025-02-03", "2026-07-10T00:00:00Z"),
        ("2026-07-10", "2026-07-10"),
        ("2026-07-11", "2026-07-10"),
    ],
)
def test_request_rejects_non_date_or_non_increasing_exclusive_range(
    tmp_path: Path,
    start: str,
    end: str,
) -> None:
    with pytest.raises(ValueError, match="start|end|exclusive"):
        BatchRequest.from_toml(
            _write_request(tmp_path / "request.toml", start=start, end=end),
        )


@pytest.mark.parametrize("symbols", [("NVDA", "NVDA"), ("NVDA", ""), ()])
def test_request_rejects_duplicate_empty_or_absent_symbols(
    tmp_path: Path,
    symbols: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="symbol"):
        BatchRequest.from_toml(
            _write_request(tmp_path / "request.toml", symbols=symbols),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("encoding", "parquet"),
        ("compression", "gzip"),
        ("split_duration", "quarter"),
    ],
)
def test_request_rejects_unsupported_batch_customizations(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    kwargs = {field: value}
    with pytest.raises(ValueError, match=field):
        BatchRequest.from_toml(
            _write_request(tmp_path / "request.toml", **kwargs),
        )


def test_request_requires_string_decimal_cost_cap(tmp_path: Path) -> None:
    request_path = _write_request(tmp_path / "request.toml")
    text = request_path.read_text(encoding="utf-8").replace(
        'max_cost_usd = "1.00"',
        "max_cost_usd = 1.00",
    )
    request_path.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError, match="max_cost_usd.*string"):
        BatchRequest.from_toml(request_path)


def test_absolute_cost_ceiling_is_immutable_one_usd() -> None:
    assert acquisition_module.ABSOLUTE_MAX_COST_USD == Decimal("1.00")


def test_request_rejects_parsed_cap_above_absolute_before_client_construction(
    tmp_path: Path,
) -> None:
    client_factory_calls: list[str] = []

    with pytest.raises(ValueError, match=r"absolute.*1\.00"):
        request = BatchRequest.from_toml(
            _write_request(tmp_path / "request.toml", max_cost_usd="1.01"),
        )
        preflight_request(
            "db-TEST-SECRET",
            request,
            client_factory=lambda key: client_factory_calls.append(key),
        )

    assert client_factory_calls == []


def test_request_rejects_direct_construction_and_replace_above_absolute(
    tmp_path: Path,
) -> None:
    request = BatchRequest.from_toml(_write_request(tmp_path / "request.toml"))
    direct_fields = dict(request.__dict__)
    direct_fields["max_cost_usd"] = Decimal("1.01")

    with pytest.raises(ValueError, match=r"absolute.*1\.00"):
        BatchRequest(**direct_fields)
    with pytest.raises(ValueError, match=r"absolute.*1\.00"):
        replace(request, max_cost_usd=Decimal("1.01"))


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("dataset", 1, "dataset must be a non-empty string"),
        ("schema", "parquet", "unsupported schema"),
        ("start", "20250203", "exact ISO dates"),
        ("end", "2025-02-03", "exclusive end"),
        ("symbols", [], "symbols must be a non-empty"),
        ("symbols", ["NVDA", "NVDA"], "symbols must not contain duplicates"),
        ("symbols", ["NVDA", 7], "symbols must contain non-empty strings"),
        ("stype_in", "ticker", "unsupported stype_in"),
        ("stype_out", "ticker", "unsupported stype_out"),
        ("encoding", "parquet", "unsupported encoding"),
        ("compression", "gzip", "unsupported compression"),
        ("pretty_px", 1, "pretty_px must be a boolean"),
        ("pretty_ts", 0, "pretty_ts must be a boolean"),
        ("map_symbols", None, "map_symbols must be a boolean"),
        ("split_symbols", "false", "split_symbols must be a boolean"),
        ("split_duration", "hour", "unsupported split_duration"),
        ("delivery", "email", "unsupported delivery"),
        ("max_cost_usd", "1.00", "max_cost_usd must be a Decimal"),
        ("symbol_source_path", 1, "symbol_source_path must be a non-empty string"),
        ("symbol_source_sha256", "A" * 64, "lowercase SHA-256"),
        ("symbol_count", True, "symbol_count must be an integer"),
        ("symbol_count", 3, "symbol_count must match"),
    ],
)
def test_request_direct_construction_and_replace_enforce_every_invariant(
    tmp_path: Path,
    field_name: str,
    invalid_value: object,
    message: str,
) -> None:
    request = _request(tmp_path)
    values = {
        item.name: getattr(request, item.name)
        for item in dataclasses.fields(BatchRequest)
    }
    values[field_name] = invalid_value

    with pytest.raises(ValueError, match=message):
        BatchRequest(**values)
    with pytest.raises(ValueError, match=message):
        replace(request, **{field_name: invalid_value})


def test_request_defensively_freezes_a_caller_owned_symbol_list(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    caller_symbols = ["NVDA", "AMD"]

    copied = replace(request, symbols=caller_symbols)
    caller_symbols.append("AAPL")

    assert copied.symbols == ("NVDA", "AMD")
    assert type(copied.symbols) is tuple


def test_request_enforces_explicit_symbol_count_ceiling(tmp_path: Path) -> None:
    request = _request(tmp_path)
    maximum = tuple(f"S{index}" for index in range(2_000))

    accepted = replace(request, symbols=maximum, symbol_count=len(maximum))

    assert accepted.symbols == maximum
    with pytest.raises(ValueError, match="at most 2000"):
        replace(
            request,
            symbols=(*maximum, "ONE_TOO_MANY"),
            symbol_count=len(maximum) + 1,
        )


def test_request_symbol_limit_counts_utf8_bytes_not_code_points(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    exactly_128_bytes = "é" * 64
    over_128_bytes = "é" * 65

    accepted = replace(
        request,
        symbols=(exactly_128_bytes,),
        symbol_count=1,
    )

    assert len(accepted.symbols[0]) == 64
    assert len(accepted.symbols[0].encode("utf-8")) == 128
    with pytest.raises(ValueError, match="128 UTF-8 bytes"):
        replace(request, symbols=(over_128_bytes,), symbol_count=1)


@pytest.mark.parametrize(
    ("marker", "injected"),
    [
        (None, '\n[credentials]\napi_key = "db-INJECTED"\n'),
        ("[request]\n", '[request]\napi_key = "db-INJECTED"\n'),
        ("[guard]\n", '[guard]\nsoft_limit = "0.50"\n'),
        ("[provenance]\n", '[provenance]\ntoken = "INJECTED"\n'),
    ],
)
def test_request_toml_rejects_unknown_root_and_nested_fields(
    tmp_path: Path,
    marker: str | None,
    injected: str,
) -> None:
    request_path = _write_request(tmp_path / "request.toml")
    original = request_path.read_text(encoding="utf-8")
    changed = (
        original + injected if marker is None else original.replace(marker, injected)
    )
    request_path.write_text(changed, encoding="utf-8")

    with pytest.raises(ValueError, match="unknown.*(section|field)"):
        BatchRequest.from_toml(request_path)


def test_request_fingerprint_is_stable_canonical_sha256(tmp_path: Path) -> None:
    first = BatchRequest.from_toml(_write_request(tmp_path / "first.toml"))
    second_path = _write_request(tmp_path / "second.toml")
    second_path.write_text(
        second_path.read_text(encoding="utf-8").replace(
            '[request]\ndataset = "XNAS.ITCH"\nschema = "statistics"',
            '[request]\nschema = "statistics"\ndataset = "XNAS.ITCH"',
        ),
        encoding="utf-8",
    )
    second = BatchRequest.from_toml(second_path)

    fingerprint = first.fingerprint()
    assert fingerprint == second.fingerprint()
    assert len(fingerprint) == 64
    assert fingerprint == hashlib.sha256(first.canonical_json()).hexdigest()


def test_request_serialization_contains_no_secret_like_fields(tmp_path: Path) -> None:
    request = BatchRequest.from_toml(_write_request(tmp_path / "request.toml"))

    serialized = request.canonical_json().decode("utf-8").lower()
    for forbidden in ("api_key", "apikey", "credential", "password", "secret", "token"):
        assert forbidden not in serialized


NOW = datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc)


def _complete_conditions(
    start: str = "2025-02-03",
    end: str = "2026-07-10",
) -> list[dict[str, object]]:
    cursor = datetime.fromisoformat(start).date()
    exclusive_end = datetime.fromisoformat(end).date()
    conditions: list[dict[str, object]] = []
    while cursor < exclusive_end:
        conditions.append(
            {"date": cursor.isoformat(), "condition": "available"},
        )
        cursor += timedelta(days=1)
    return conditions


class _FakeMetadata:
    def __init__(self, owner: "_FakeClient") -> None:
        self.owner = owner

    def get_dataset_range(self, dataset: object) -> dict[str, object]:
        self.owner.calls.append(("get_dataset_range", {"dataset": dataset}))
        return dict(self.owner.dataset_range)

    def list_schemas(self, dataset: object) -> list[str]:
        self.owner.calls.append(("list_schemas", {"dataset": dataset}))
        return list(self.owner.available_schemas)

    def get_dataset_condition(
        self,
        dataset: object,
        start_date: object | None = None,
        end_date: object | None = None,
    ) -> list[dict[str, object]]:
        self.owner.calls.append(
            (
                "get_dataset_condition",
                {
                    "dataset": dataset,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            ),
        )
        if self.owner.conditions is None:
            assert type(start_date) is str
            assert type(end_date) is str
            exclusive_end = (
                date.fromisoformat(end_date) + timedelta(days=1)
            ).isoformat()
            return _complete_conditions(start_date, exclusive_end)
        return [dict(item) for item in self.owner.conditions]

    def get_cost(
        self,
        dataset: object,
        start: object,
        end: object | None = None,
        mode: object | None = None,
        symbols: object | None = None,
        schema: object = "trades",
        stype_in: object = "raw_symbol",
        limit: int | None = None,
    ) -> float:
        kwargs = {
            "dataset": dataset,
            "start": start,
            "end": end,
            "symbols": symbols,
            "schema": schema,
            "stype_in": stype_in,
        }
        if mode is not None:
            kwargs["mode"] = mode
        if limit is not None:
            kwargs["limit"] = limit
        self.owner.calls.append(("get_cost", kwargs))
        if self.owner.requote_hook is not None:
            self.owner.requote_hook()
        return self.owner.costs.pop(0)

    def get_record_count(
        self,
        dataset: object,
        start: object,
        end: object | None = None,
        symbols: object | None = None,
        schema: object = "trades",
        stype_in: object = "raw_symbol",
        limit: int | None = None,
    ) -> int:
        kwargs = {
            "dataset": dataset,
            "start": start,
            "end": end,
            "symbols": symbols,
            "schema": schema,
            "stype_in": stype_in,
        }
        if limit is not None:
            kwargs["limit"] = limit
        self.owner.calls.append(("get_record_count", kwargs))
        return self.owner.record_count

    def get_billable_size(
        self,
        dataset: object,
        start: object,
        end: object | None = None,
        symbols: object | None = None,
        schema: object = "trades",
        stype_in: object = "raw_symbol",
        limit: int | None = None,
    ) -> int:
        kwargs = {
            "dataset": dataset,
            "start": start,
            "end": end,
            "symbols": symbols,
            "schema": schema,
            "stype_in": stype_in,
        }
        if limit is not None:
            kwargs["limit"] = limit
        self.owner.calls.append(("get_billable_size", kwargs))
        return self.owner.billable_size


class _FakeBatch:
    def __init__(self, owner: "_FakeClient") -> None:
        self.owner = owner

    def submit_job(
        self,
        dataset: object,
        symbols: object,
        schema: object,
        start: object,
        end: object | None = None,
        encoding: object = "dbn",
        compression: object = "zstd",
        pretty_px: bool = False,
        pretty_ts: bool = False,
        map_symbols: bool | None = None,
        split_symbols: bool = False,
        split_duration: object = "day",
        split_size: int | None = None,
        delivery: object = "download",
        stype_in: object = "raw_symbol",
        stype_out: object = "instrument_id",
        limit: int | None = None,
    ) -> dict[str, object]:
        kwargs = {
            "dataset": dataset,
            "symbols": symbols,
            "schema": schema,
            "start": start,
            "end": end,
            "encoding": encoding,
            "compression": compression,
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
            "split_symbols": split_symbols,
            "split_duration": split_duration,
            "delivery": delivery,
            "stype_in": stype_in,
            "stype_out": stype_out,
        }
        if split_size is not None:
            kwargs["split_size"] = split_size
        if limit is not None:
            kwargs["limit"] = limit
        self.owner.calls.append(("submit_job", kwargs))
        if self.owner.submit_hook is not None:
            self.owner.submit_hook()
        if self.owner.submit_error is not None:
            raise self.owner.submit_error
        return dict(self.owner.submit_response)


class _FakeClient:
    sdk_version = "0.81.0-test"

    def __init__(
        self,
        *,
        costs: tuple[float, ...] = (0.25,),
        dataset_range: dict[str, object] | None = None,
        available_schemas: tuple[str, ...] = ("statistics", "status"),
        conditions: tuple[dict[str, object], ...] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.costs = list(costs)
        self.dataset_range = dataset_range or {
            "start": "2018-05-01",
            "end": "2026-08-01",
            "schema": {
                "statistics": {"start": "2018-05-01", "end": "2026-08-01"},
                "status": {"start": "2018-05-01", "end": "2026-08-01"},
            },
        }
        self.available_schemas = available_schemas
        self.conditions = None if conditions is None else list(conditions)
        self.record_count = 1234
        self.billable_size = 5678
        self.submit_response = {"id": "XNAS-TEST-JOB", "state": "queued"}
        self.requote_hook = None
        self.submit_hook = None
        self.submit_error: BaseException | None = None
        self.metadata = _FakeMetadata(self)
        self.batch = _FakeBatch(self)


def _spawn_submit_worker(
    tag: str,
    request_path: str,
    plan_path: str,
    state_root: str,
    start_event: Any,
    requote_ready_event: Any,
    requote_release_event: Any,
    result_queue: Any,
    hold_during_requote: bool,
) -> None:
    """Run one fake-client submission in a fresh spawned interpreter."""
    os.environ["DATABENTO_INGEST_STATE_DIR"] = state_root
    request = BatchRequest.from_toml(Path(request_path))
    plan = load_plan(Path(plan_path))
    client = _FakeClient(costs=(0.30,))
    factory_calls: list[str] = []

    def client_factory(api_key: str) -> _FakeClient:
        factory_calls.append(api_key)
        return client

    if hold_during_requote:

        def hold_requote() -> None:
            requote_ready_event.set()
            if not requote_release_event.wait(15):
                raise TimeoutError("spawned requote release timed out")

        client.requote_hook = hold_requote

    try:
        if not start_event.wait(15):
            raise TimeoutError("spawned submission start timed out")
        receipt = submit_planned_request(
            "db-SPAWN-TEST",
            request,
            plan,
            client_factory=client_factory,
            clock=lambda: NOW + timedelta(minutes=2),
        )
    except BaseException as exc:
        result_queue.put(
            {
                "tag": tag,
                "status": (
                    "refused" if isinstance(exc, AcquisitionRefused) else "error"
                ),
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "factory_calls": len(factory_calls),
                "call_names": [name for name, _ in client.calls],
                "submit_count": [name for name, _ in client.calls].count("submit_job"),
            }
        )
        return
    result_queue.put(
        {
            "tag": tag,
            "status": "submitted",
            "job_id": receipt["job_id"],
            "factory_calls": len(factory_calls),
            "call_names": [name for name, _ in client.calls],
            "submit_count": [name for name, _ in client.calls].count("submit_job"),
        }
    )


def _spawn_reserve_then_exit(
    request_path: str,
    state_root: str,
    durable_event: Any,
    status_value: Any,
) -> None:
    """Simulate process death immediately after a durable reservation."""
    os.environ["DATABENTO_INGEST_STATE_DIR"] = state_root
    try:
        request = BatchRequest.from_toml(Path(request_path))
        ledger, _ = acquisition_module._reserve_submission_identity(
            request.submission_identity(),
            request.submission(),
            NOW + timedelta(minutes=1),
        )
        assert ledger.record_fd is not None
        status_value.value = 1
        durable_event.set()
        os._exit(0)
    except BaseException:
        status_value.value = -1
        durable_event.set()
        os._exit(2)


def _join_spawned_process(process: Any, label: str) -> None:
    process.join(15)
    if process.is_alive():
        process.terminate()
        process.join(5)
        pytest.fail(f"{label} did not exit")
    assert process.exitcode == 0, f"{label} exit code: {process.exitcode}"


def _request(
    tmp_path: Path, *, symbols: tuple[str, ...] = ("NVDA", "AMD")
) -> BatchRequest:
    return BatchRequest.from_toml(
        _write_request(
            tmp_path / f"request-{len(list(tmp_path.iterdir()))}.toml", symbols=symbols
        ),
    )


def _preflight(
    tmp_path: Path,
    *,
    client: _FakeClient | None = None,
    request: BatchRequest | None = None,
) -> tuple[BatchRequest, BatchPlan, _FakeClient]:
    selected_client = client or _FakeClient()
    selected_request = request or _request(tmp_path)
    plan = preflight_request(
        "db-TEST-SECRET",
        selected_request,
        client_factory=lambda key: selected_client,
        now=NOW,
    )
    return selected_request, plan, selected_client


def _rehash_plan_payload(raw: dict[str, object]) -> None:
    unsigned = {key: value for key, value in raw.items() if key != "plan_sha256"}
    raw["plan_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    ).hexdigest()


def _submission_state_records() -> list[Path]:
    root = Path(os.environ["DATABENTO_INGEST_STATE_DIR"])
    return sorted((root / "submissions").glob("*.json"))


def _submission_journal(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _last_submission_state(path: Path) -> dict[str, object]:
    return _submission_journal(path)[-1]


def _replace_with_intruder(path: Path, record: dict[str, object]) -> None:
    intruder = path.with_name(f".{path.name}.intruder")
    intruder.write_text(
        json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    intruder.chmod(0o600)
    os.replace(intruder, path)


def test_journal_append_accepts_exact_one_mib_projected_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "exact-boundary.json"
    record = {"state": "test", "value": "é"}
    payload = acquisition_module._canonical_json(record) + b"\n"
    path.write_bytes(b"x" * ((1024 * 1024) - len(payload)))
    path.chmod(0o600)
    fd = os.open(path, os.O_RDWR | os.O_APPEND)
    try:
        acquisition_module._append_record_payload(fd, record)
    finally:
        os.close(fd)

    assert path.stat().st_size == 1024 * 1024
    assert path.read_bytes().endswith(payload)


def test_journal_append_refuses_one_byte_projected_overflow_without_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "one-byte-over.json"
    record = {"state": "test", "value": "é"}
    payload = acquisition_module._canonical_json(record) + b"\n"
    original = b"x" * ((1024 * 1024) - len(payload) + 1)
    path.write_bytes(original)
    path.chmod(0o600)
    fd = os.open(path, os.O_RDWR | os.O_APPEND)
    try:
        with pytest.raises(AcquisitionRefused, match="capacity exhausted"):
            acquisition_module._append_record_payload(fd, record)
    finally:
        os.close(fd)

    assert path.read_bytes() == original


def test_preflight_calls_exact_official_metadata_queries(tmp_path: Path) -> None:
    request, plan, client = _preflight(tmp_path)

    query = request.provider_query()
    assert client.calls == [
        ("get_dataset_range", {"dataset": "XNAS.ITCH"}),
        ("list_schemas", {"dataset": "XNAS.ITCH"}),
        (
            "get_dataset_condition",
            {
                "dataset": "XNAS.ITCH",
                "start_date": "2025-02-03",
                "end_date": "2026-07-09",
            },
        ),
        ("get_cost", query),
        ("get_record_count", query),
        ("get_billable_size", query),
    ]
    assert plan.request_fingerprint == request.fingerprint()
    assert plan.estimated_cost_usd == Decimal("0.25")
    assert plan.estimated_record_count == 1234
    assert plan.estimated_billable_bytes == 5678
    assert plan.estimated_at == NOW
    assert plan.available_schemas == ("statistics", "status")
    assert plan.availability_ok is True
    serialized = plan.canonical_json().decode("utf-8")
    assert "db-TEST-SECRET" not in serialized
    assert (
        plan.plan_sha256 == hashlib.sha256(plan.unsigned_canonical_json()).hexdigest()
    )


def test_submit_refuses_plan_estimate_above_cap_without_submission(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(1.01,))
    request, plan, client = _preflight(tmp_path, client=client)

    with pytest.raises(AcquisitionRefused, match="estimate.*cap"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0


def test_plan_create_parse_and_replace_reject_cap_above_absolute(
    tmp_path: Path,
) -> None:
    request, plan, _ = _preflight(tmp_path)
    raw = plan.to_dict()
    raw["max_cost_usd"] = "1.01"

    with pytest.raises(ValueError, match=r"absolute.*1\.00"):
        BatchPlan.from_dict(raw)
    with pytest.raises(ValueError, match=r"absolute.*1\.00"):
        replace(plan, max_cost_usd=Decimal("1.01"))

    object.__setattr__(request, "max_cost_usd", Decimal("1.01"))
    with pytest.raises(ValueError, match=r"absolute.*1\.00"):
        BatchPlan.create(
            request=request,
            estimated_at=plan.estimated_at,
            sdk_version=plan.sdk_version,
            dataset_range=plan.dataset_range,
            available_schemas=list(plan.available_schemas),
            dataset_conditions=[dict(item) for item in plan.dataset_conditions],
            estimated_cost_usd=plan.estimated_cost_usd,
            estimated_record_count=plan.estimated_record_count,
            estimated_billable_bytes=plan.estimated_billable_bytes,
            availability_reasons=list(plan.availability_reasons),
        )


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"schema_version": "9.9"}, "schema"),
        ({"schema_version": 1}, "schema"),
        ({"request_fingerprint": "A" * 64}, "request_fingerprint"),
        ({"request_fingerprint": 7}, "request_fingerprint"),
        ({"estimated_at": NOW.replace(tzinfo=None)}, "estimated_at"),
        ({"estimated_at": "2026-07-22T12:00:00Z"}, "estimated_at"),
        ({"sdk_version": ""}, "sdk_version"),
        ({"sdk_version": 81}, "sdk_version"),
        ({"estimated_cost_usd": Decimal("-0.01")}, "estimated_cost_usd"),
        ({"estimated_cost_usd": Decimal("NaN")}, "estimated_cost_usd"),
        ({"estimated_cost_usd": -0.01}, "estimated_cost_usd"),
        ({"estimated_record_count": -1}, "estimated_record_count"),
        ({"estimated_record_count": True}, "estimated_record_count"),
        ({"estimated_record_count": 1.5}, "estimated_record_count"),
        ({"estimated_billable_bytes": -1}, "estimated_billable_bytes"),
        ({"estimated_billable_bytes": True}, "estimated_billable_bytes"),
        ({"estimated_billable_bytes": 1.5}, "estimated_billable_bytes"),
        ({"plan_sha256": "A" * 64}, "plan_sha256"),
        ({"plan_sha256": 7}, "plan_sha256"),
    ],
)
def test_batch_plan_direct_and_replace_paths_enforce_every_scalar_invariant(
    tmp_path: Path,
    changes: dict[str, object],
    match: str,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    values = {
        field.name: getattr(plan, field.name) for field in dataclasses.fields(plan)
    }
    values.update(changes)

    with pytest.raises(ValueError, match=match):
        BatchPlan(**values)
    with pytest.raises(ValueError, match=match):
        replace(plan, **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": "9.9"},
        {"sdk_version": ""},
        {"estimated_cost_usd": Decimal("-0.01")},
        {"estimated_record_count": -1},
        {"estimated_record_count": True},
        {"estimated_billable_bytes": -1},
        {"estimated_billable_bytes": 1.5},
    ],
)
def test_rehashed_invalid_plan_never_constructs_client_or_calls_provider(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    request, plan, client = _preflight(tmp_path)
    calls_before = list(client.calls)
    factory_calls: list[str] = []

    def client_factory(api_key: str) -> _FakeClient:
        factory_calls.append(api_key)
        return client

    with pytest.raises((ValueError, AcquisitionRefused)):
        invalid = replace(plan, **changes)
        object.__setattr__(
            invalid,
            "plan_sha256",
            hashlib.sha256(invalid.unsigned_canonical_json()).hexdigest(),
        )
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            invalid,
            client_factory=client_factory,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert factory_calls == []
    assert client.calls == calls_before


def test_submit_independently_refuses_forged_cap_above_absolute_without_provider_call(
    tmp_path: Path,
) -> None:
    request, plan, client = _preflight(tmp_path)
    calls_before_submit = list(client.calls)
    object.__setattr__(request, "max_cost_usd", Decimal("1.01"))

    with pytest.raises(AcquisitionRefused, match=r"absolute.*1\.00"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert client.calls == calls_before_submit


def test_submit_refuses_plan_older_than_15_minutes_without_submission(
    tmp_path: Path,
) -> None:
    request, plan, client = _preflight(tmp_path)

    with pytest.raises(AcquisitionRefused, match="older than 15 minutes"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=15, microseconds=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0


def test_submit_rechecks_plan_age_after_requote_before_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    moments = iter(
        (
            NOW + timedelta(minutes=14),
            NOW + timedelta(minutes=15, microseconds=1),
        ),
    )

    class _Clock:
        @classmethod
        def now(cls, tz: object) -> datetime:
            assert tz is timezone.utc
            return next(moments)

    monkeypatch.setattr(acquisition_module, "datetime", _Clock)

    with pytest.raises(AcquisitionRefused, match="older than 15 minutes"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
        )

    assert [name for name, _ in client.calls].count("get_cost") == 2
    assert [name for name, _ in client.calls].count("submit_job") == 0


def test_submit_aborts_if_plan_expires_during_attempted_state_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    current = {"value": NOW + timedelta(minutes=14)}

    class _Clock:
        @classmethod
        def now(cls, tz: object) -> datetime:
            assert tz is timezone.utc
            return current["value"]

    original_replace = acquisition_module._replace_submission_state

    def persist_then_advance(
        ledger: object,
        expected: dict[str, object],
        replacement: dict[str, object],
    ) -> None:
        original_replace(ledger, expected, replacement)
        if replacement["state"] == "attempted":
            current["value"] = NOW + timedelta(minutes=16)

    monkeypatch.setattr(acquisition_module, "datetime", _Clock)
    monkeypatch.setattr(
        acquisition_module,
        "_replace_submission_state",
        persist_then_advance,
    )

    with pytest.raises(AcquisitionRefused, match="older than 15 minutes"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0
    record = _last_submission_state(_submission_state_records()[0])
    assert record["state"] == "aborted"
    assert record["abort_reason"] == "plan is older than 15 minutes"


def test_submit_refuses_request_fingerprint_mismatch_without_submission(
    tmp_path: Path,
) -> None:
    request, plan, client = _preflight(tmp_path)
    different_request = _request(tmp_path, symbols=("NVDA", "AAPL"))

    with pytest.raises(AcquisitionRefused, match="fingerprint"):
        submit_planned_request(
            "db-TEST-SECRET",
            different_request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0


def test_submit_refuses_plan_provider_query_mismatch_without_submission(
    tmp_path: Path,
) -> None:
    request, plan, client = _preflight(tmp_path)
    changed = replace(plan, provider_query={**plan.provider_query, "schema": "status"})
    changed = replace(
        changed,
        plan_sha256=hashlib.sha256(changed.unsigned_canonical_json()).hexdigest(),
    )

    with pytest.raises(AcquisitionRefused, match="provider query"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            changed,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0


@pytest.mark.parametrize(
    ("dataset_range", "available_schemas", "reason"),
    [
        (
            {
                "start": "2018-05-01",
                "end": "2026-08-01",
                "schema": {
                    "statistics": {"start": "2018-05-01", "end": "2026-08-01"},
                },
            },
            ("status",),
            "schema",
        ),
        (
            {
                "start": "2018-05-01",
                "end": "2026-08-01",
                "schema": {
                    "statistics": {"start": "2025-03-01", "end": "2026-08-01"},
                },
            },
            ("statistics",),
            "range",
        ),
        (
            {
                "start": "2018-05-01",
                "end": "2026-08-01",
                "schema": {
                    "statistics": {"start": "2018-05-01", "end": "2026-07-01"},
                },
            },
            ("statistics",),
            "range",
        ),
    ],
)
def test_submit_refuses_unavailable_schema_or_range_without_submission(
    tmp_path: Path,
    dataset_range: dict[str, object],
    available_schemas: tuple[str, ...],
    reason: str,
) -> None:
    client = _FakeClient(
        dataset_range=dataset_range,
        available_schemas=available_schemas,
    )
    request, plan, client = _preflight(tmp_path, client=client)
    assert plan.availability_ok is False
    assert any(reason in item for item in plan.availability_reasons)

    with pytest.raises(AcquisitionRefused, match="availability"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0


@pytest.mark.parametrize(
    "schema_value",
    [
        None,
        [],
        {"statistics": "malformed"},
    ],
)
def test_preflight_rejects_schema_range_container_that_is_missing_or_malformed(
    tmp_path: Path,
    schema_value: object,
) -> None:
    dataset_range: dict[str, object] = {
        "start": "2018-05-01",
        "end": "2026-08-01",
    }
    if schema_value is not None:
        dataset_range["schema"] = schema_value
    client = _FakeClient(dataset_range=dataset_range)

    with pytest.raises(ValueError, match="dataset_range|schema range"):
        _preflight(tmp_path, client=client)


def test_preflight_fails_closed_when_requested_schema_range_is_missing(
    tmp_path: Path,
) -> None:
    client = _FakeClient(
        dataset_range={
            "start": "2018-05-01",
            "end": "2026-08-01",
            "schema": {
                "status": {"start": "2018-05-01", "end": "2026-08-01"},
            },
        },
    )

    _, plan, _ = _preflight(tmp_path, client=client)

    assert plan.availability_ok is False
    assert any("schema range" in reason for reason in plan.availability_reasons)


@pytest.mark.parametrize("target", ["dataset", "schema"])
def test_preflight_rejects_midday_start_for_utc_midnight_request_boundary(
    tmp_path: Path,
    target: str,
) -> None:
    dataset_range: dict[str, object] = {
        "start": "2018-05-01T00:00:00Z",
        "end": "2026-08-01T00:00:00Z",
        "schema": {
            "statistics": {
                "start": "2018-05-01T00:00:00Z",
                "end": "2026-08-01T00:00:00Z",
            },
        },
    }
    selected = (
        dataset_range if target == "dataset" else dataset_range["schema"]["statistics"]
    )
    selected["start"] = "2025-02-03T12:00:00Z"

    _, plan, _ = _preflight(tmp_path, client=_FakeClient(dataset_range=dataset_range))

    assert plan.availability_ok is False
    assert any(
        f"{target} range starts after request" in reason
        for reason in plan.availability_reasons
    )


@pytest.mark.parametrize("target", ["dataset", "schema"])
def test_preflight_normalizes_offset_end_before_comparing_exclusive_boundary(
    tmp_path: Path,
    target: str,
) -> None:
    dataset_range: dict[str, object] = {
        "start": "2018-05-01T00:00:00Z",
        "end": "2026-08-01T00:00:00Z",
        "schema": {
            "statistics": {
                "start": "2018-05-01T00:00:00Z",
                "end": "2026-08-01T00:00:00Z",
            },
        },
    }
    selected = (
        dataset_range if target == "dataset" else dataset_range["schema"]["statistics"]
    )
    selected["end"] = "2026-07-10T00:30:00+01:00"

    _, plan, _ = _preflight(tmp_path, client=_FakeClient(dataset_range=dataset_range))

    assert plan.availability_ok is False
    assert any(
        f"{target} range ends before request" in reason
        for reason in plan.availability_reasons
    )


def test_preflight_accepts_offset_ranges_equal_to_utc_request_boundaries(
    tmp_path: Path,
) -> None:
    exact_range = {
        "start": "2025-02-02T19:00:00-05:00",
        "end": "2026-07-10T02:00:00+02:00",
    }
    dataset_range: dict[str, object] = {
        **exact_range,
        "schema": {"statistics": dict(exact_range)},
    }

    _, plan, _ = _preflight(tmp_path, client=_FakeClient(dataset_range=dataset_range))

    assert plan.availability_ok is True


@pytest.mark.parametrize("target", ["dataset", "schema"])
def test_preflight_accepts_midday_end_after_exclusive_utc_boundary(
    tmp_path: Path,
    target: str,
) -> None:
    dataset_range: dict[str, object] = {
        "start": "2018-05-01T00:00:00Z",
        "end": "2026-08-01T00:00:00Z",
        "schema": {
            "statistics": {
                "start": "2018-05-01T00:00:00Z",
                "end": "2026-08-01T00:00:00Z",
            },
        },
    }
    selected = (
        dataset_range if target == "dataset" else dataset_range["schema"]["statistics"]
    )
    selected["end"] = "2026-07-10T12:00:00Z"

    _, plan, _ = _preflight(tmp_path, client=_FakeClient(dataset_range=dataset_range))

    assert plan.availability_ok is True


@pytest.mark.parametrize("blocking_condition", ["pending", "missing"])
def test_preflight_blocks_pending_or_missing_dataset_condition(
    tmp_path: Path,
    blocking_condition: str,
) -> None:
    conditions = _complete_conditions()
    conditions[1]["condition"] = blocking_condition
    client = _FakeClient(conditions=tuple(conditions))

    _, plan, _ = _preflight(tmp_path, client=client)

    assert plan.availability_ok is False
    assert any(blocking_condition in reason for reason in plan.availability_reasons)


def test_preflight_preserves_degraded_condition_without_blocking(
    tmp_path: Path,
) -> None:
    degraded = {"date": "2025-02-04", "condition": "degraded"}
    conditions = _complete_conditions()
    conditions[1] = degraded
    client = _FakeClient(conditions=tuple(conditions))

    _, plan, _ = _preflight(tmp_path, client=client)

    assert plan.availability_ok is True
    assert degraded in plan.dataset_conditions
    assert plan.availability_reasons == ()


@pytest.mark.parametrize(
    "condition_case",
    ["empty", "duplicate", "out_of_range"],
)
def test_preflight_rejects_unusable_condition_population(
    tmp_path: Path,
    condition_case: str,
) -> None:
    request = BatchRequest.from_toml(
        _write_request(
            tmp_path / "short-request.toml",
            start="2025-02-03",
            end="2025-02-06",
        ),
    )
    conditions = _complete_conditions(request.start, request.end)
    if condition_case == "empty":
        conditions = []
    elif condition_case == "duplicate":
        conditions.insert(1, dict(conditions[0]))
    else:
        conditions.insert(
            0,
            {"date": "2025-02-02", "condition": "available"},
        )

    with pytest.raises(
        ValueError, match="dataset condition.*(provider row|duplicate|range)"
    ):
        _preflight(
            tmp_path,
            request=request,
            client=_FakeClient(conditions=tuple(conditions)),
        )


def test_preflight_preserves_sparse_provider_rows_and_marks_coverage_unverified(
    tmp_path: Path,
) -> None:
    request = BatchRequest.from_toml(
        _write_request(
            tmp_path / "weekend-request.toml",
            start="2025-09-12",
            end="2025-09-16",
        ),
    )
    # A held provider artifact establishes that condition rows can skip one
    # weekend date while returning another; row granularity is not density.
    # The generic contract also preserves provider order without inventing an
    # undocumented ordering requirement.
    conditions = [
        {"date": "2025-09-14", "condition": "available"},
        {"date": "2025-09-12", "condition": "available"},
        {"date": "2025-09-15", "condition": "available"},
    ]

    _, plan, _ = _preflight(
        tmp_path,
        request=request,
        client=_FakeClient(conditions=tuple(conditions)),
    )

    assert plan.availability_ok is True
    assert plan.condition_coverage == "provider_rows_unverified"
    assert plan.to_dict()["dataset_conditions"] == conditions


@pytest.mark.parametrize("blocking_condition", ["pending", "missing"])
def test_plan_parser_rejects_rehashed_blocking_condition_with_false_availability(
    tmp_path: Path,
    blocking_condition: str,
) -> None:
    request = BatchRequest.from_toml(
        _write_request(
            tmp_path / "short-request.toml",
            start="2025-02-03",
            end="2025-02-06",
        ),
    )
    _, plan, _ = _preflight(tmp_path, request=request)
    raw = plan.to_dict()
    raw["dataset_conditions"][1]["condition"] = blocking_condition
    raw["availability_ok"] = True
    raw["availability_reasons"] = []
    _rehash_plan_payload(raw)

    with pytest.raises(ValueError, match="derived availability"):
        BatchPlan.from_dict(raw)


@pytest.mark.parametrize("range_target", ["dataset", "schema"])
def test_plan_parser_rejects_rehashed_narrow_range_with_false_availability(
    tmp_path: Path,
    range_target: str,
) -> None:
    request = BatchRequest.from_toml(
        _write_request(
            tmp_path / "short-request.toml",
            start="2025-02-03",
            end="2025-02-06",
        ),
    )
    _, plan, _ = _preflight(tmp_path, request=request)
    raw = plan.to_dict()
    selected = (
        raw["dataset_range"]
        if range_target == "dataset"
        else raw["dataset_range"]["schema"]["statistics"]
    )
    selected["end"] = "2025-02-05T23:59:59Z"
    raw["availability_ok"] = True
    raw["availability_reasons"] = []
    _rehash_plan_payload(raw)

    with pytest.raises(ValueError, match="derived availability"):
        BatchPlan.from_dict(raw)


@pytest.mark.parametrize("forgery", ["missing_condition", "narrow_schema_range"])
def test_submit_recomputes_availability_and_refuses_forged_plan_without_provider(
    tmp_path: Path,
    forgery: str,
) -> None:
    request = BatchRequest.from_toml(
        _write_request(
            tmp_path / "short-request.toml",
            start="2025-02-03",
            end="2025-02-06",
        ),
    )
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, request=request, client=client)
    if forgery == "missing_condition":
        conditions = plan.to_dict()["dataset_conditions"]
        conditions[1]["condition"] = "missing"
        object.__setattr__(plan, "dataset_conditions", tuple(conditions))
    else:
        dataset_range = plan.to_dict()["dataset_range"]
        dataset_range["schema"]["statistics"]["end"] = "2025-02-05T23:59:59Z"
        object.__setattr__(plan, "dataset_range", dataset_range)
    object.__setattr__(
        plan,
        "plan_sha256",
        hashlib.sha256(plan.unsigned_canonical_json()).hexdigest(),
    )
    calls_before = list(client.calls)

    with pytest.raises(AcquisitionRefused, match="derived availability"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert client.calls == calls_before


def test_submit_refuses_immediate_requote_above_cap_without_submission(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 1.01))
    request, plan, client = _preflight(tmp_path, client=client)

    with pytest.raises(AcquisitionRefused, match="re-quote.*cap"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0


def test_submit_requotes_then_submits_once_with_complete_customization_tuple(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)

    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    submit_calls = [kwargs for name, kwargs in client.calls if name == "submit_job"]
    assert submit_calls == [
        {
            **request.provider_query(),
            "stype_out": "instrument_id",
            "encoding": "dbn",
            "compression": "zstd",
            "pretty_px": False,
            "pretty_ts": False,
            "map_symbols": False,
            "split_symbols": False,
            "split_duration": "day",
            "delivery": "download",
        },
    ]
    assert receipt["job_id"] == "XNAS-TEST-JOB"
    assert receipt["provider_response"] == {"id": "XNAS-TEST-JOB", "state": "queued"}
    assert receipt["request_fingerprint"] == request.fingerprint()
    assert receipt["plan_sha256"] == plan.plan_sha256
    assert receipt["requoted_cost_usd"] == "0.3"
    assert receipt["submitted_at"] == "2026-07-22T12:01:00Z"
    receipt_hash = receipt["receipt_sha256"]
    unsigned = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    assert (
        receipt_hash
        == hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        ).hexdigest()
    )
    assert "db-TEST-SECRET" not in json.dumps(receipt)


def test_submit_receipt_uses_timestamp_captured_after_requote(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    moments = iter(
        (
            NOW + timedelta(minutes=1),
            NOW + timedelta(minutes=2),
            NOW + timedelta(minutes=3),
            NOW + timedelta(minutes=4),
        ),
    )

    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: next(moments),
    )

    assert receipt["submitted_at"] == "2026-07-22T12:03:00Z"
    state_record = _last_submission_state(_submission_state_records()[0])
    assert state_record["reserved_at"] == "2026-07-22T12:01:00Z"
    assert state_record["attempted_at"] == "2026-07-22T12:02:00Z"
    assert state_record["consumed_at"] == "2026-07-22T12:04:00Z"


def test_submit_rejects_scalar_clock_before_ledger_or_provider_construction(
    tmp_path: Path,
) -> None:
    request, plan, client = _preflight(tmp_path)
    calls_before = list(client.calls)
    factory_calls: list[str] = []

    with pytest.raises(TypeError, match="clock.*callable"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: factory_calls.append(key),
            clock=NOW,  # type: ignore[arg-type]
        )

    assert factory_calls == []
    assert client.calls == calls_before
    assert _submission_state_records() == []


def test_submit_sanitizes_provider_shaped_response_before_receipt_and_hash(
    tmp_path: Path,
) -> None:
    api_key = "db-PROVIDER-RETURNED-SECRET"
    client = _FakeClient(costs=(0.25, 0.30))
    client.submit_response = {
        "id": "XNAS-TEST-JOB",
        "user_id": "user-sensitive-account-id",
        "api_key": api_key,
        "state": "queued",
        "symbols": ["NVDA", "AMD"],
        "cost_usd": 0.30,
        "record_count": 1234,
        "billed_size": 5678,
        "package_size": 0,
        "ts_received": "2026-07-22T12:01:00Z",
        "ts_queued": None,
        "unreviewed_nested": {"token": "NESTED-SECRET"},
    }
    request, plan, client = _preflight(tmp_path, client=client)

    receipt = submit_planned_request(
        api_key,
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    assert receipt["provider_response"] == {
        "id": "XNAS-TEST-JOB",
        "state": "queued",
        "symbols": ["NVDA", "AMD"],
        "cost_usd": 0.30,
        "record_count": 1234,
        "billed_size": 5678,
        "package_size": 0,
        "ts_received": "2026-07-22T12:01:00Z",
        "ts_queued": None,
    }

    def recursive_keys(value: object) -> list[str]:
        if isinstance(value, dict):
            return [
                *(str(key).lower() for key in value),
                *(key for item in value.values() for key in recursive_keys(item)),
            ]
        if isinstance(value, list):
            return [key for item in value for key in recursive_keys(item)]
        return []

    forbidden_keys = {
        "api_key",
        "user_id",
        "account_id",
        "credential",
        "password",
        "secret",
        "token",
    }
    assert forbidden_keys.isdisjoint(recursive_keys(receipt))
    serialized = json.dumps(receipt, sort_keys=True)
    assert api_key not in serialized
    assert "user-sensitive-account-id" not in serialized
    assert "NESTED-SECRET" not in serialized
    unsigned = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    assert (
        receipt["receipt_sha256"]
        == hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        ).hexdigest()
    )


def test_submit_accepts_job_id_at_256_utf8_byte_boundary(tmp_path: Path) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    client.submit_response = {"id": "é" * 128, "state": "queued"}
    request, plan, client = _preflight(tmp_path, client=client)

    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    assert len(str(receipt["job_id"]).encode("utf-8")) == 256
    assert _last_submission_state(_submission_state_records()[0])["state"] == "consumed"


def test_submit_rejects_job_id_over_256_utf8_bytes_and_preserves_attempt(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    client.submit_response = {"id": "é" * 129, "state": "queued"}
    request, plan, client = _preflight(tmp_path, client=client)

    with pytest.raises(ValueError, match="job ID.*256 UTF-8 bytes"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 1
    assert (
        _last_submission_state(_submission_state_records()[0])["state"] == "attempted"
    )


def test_submission_identity_covers_exact_provider_tuple_not_cap_or_provenance(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    same_provider_action = replace(
        request,
        max_cost_usd=Decimal("0.50"),
        symbol_source_path="/different/sealed/source.json",
        symbol_source_sha256="b" * 64,
    )

    expected = hashlib.sha256(
        json.dumps(
            request.submission(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ).hexdigest()

    assert request.submission_identity() == expected
    assert same_provider_action.submission_identity() == expected
    assert same_provider_action.fingerprint() != request.fingerprint()


def test_default_submission_state_root_is_fixed_per_user(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABENTO_INGEST_STATE_DIR")
    monkeypatch.setenv("HOME", str(tmp_path))

    assert acquisition_module._submission_state_root() == (
        tmp_path / ".local/state/databento-ingest"
    )


def test_submission_state_override_must_resolve_to_an_absolute_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABENTO_INGEST_STATE_DIR", "relative/submission-state")

    with pytest.raises(ValueError, match="must be an absolute path"):
        acquisition_module._submission_state_root()


@pytest.mark.parametrize("target_kind", ["root", "submissions"])
def test_submission_ledger_rejects_symlink_directories_before_provider_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    state_root = Path(os.environ["DATABENTO_INGEST_STATE_DIR"])
    real_directory = tmp_path / f"real-{target_kind}"
    real_directory.mkdir(mode=0o700)
    real_directory.chmod(0o700)
    if target_kind == "root":
        state_root.symlink_to(real_directory, target_is_directory=True)
    else:
        state_root.mkdir(mode=0o700)
        state_root.chmod(0o700)
        (state_root / "submissions").symlink_to(
            real_directory,
            target_is_directory=True,
        )
    calls_before = list(client.calls)

    with pytest.raises(AcquisitionRefused, match="ledger.*(symlink|directory)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert client.calls == calls_before


@pytest.mark.parametrize("target_kind", ["root", "submissions"])
def test_submission_ledger_rejects_and_never_chmods_existing_wrong_mode_directory(
    tmp_path: Path,
    target_kind: str,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    state_root = Path(os.environ["DATABENTO_INGEST_STATE_DIR"])
    state_root.mkdir(mode=0o700)
    state_root.chmod(0o700)
    target = state_root
    if target_kind == "submissions":
        target = state_root / "submissions"
        target.mkdir(mode=0o755)
    target.chmod(0o755)
    calls_before = list(client.calls)

    with pytest.raises(AcquisitionRefused, match="ledger.*mode"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert stat.S_IMODE(target.stat().st_mode) == 0o755
    assert client.calls == calls_before


def test_submission_ledger_rejects_non_directory_path_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    component = tmp_path / "not-a-directory"
    component.write_text("held\n", encoding="utf-8")
    monkeypatch.setenv(
        "DATABENTO_INGEST_STATE_DIR",
        str(component / "submission-state"),
    )
    calls_before = list(client.calls)

    with pytest.raises(AcquisitionRefused, match="ledger.*directory"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert component.read_text(encoding="utf-8") == "held\n"
    assert client.calls == calls_before


def test_submission_ledger_rejects_symlinked_intermediate_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir(mode=0o700)
    real_parent.chmod(0o700)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    monkeypatch.setenv(
        "DATABENTO_INGEST_STATE_DIR",
        str(linked_parent / "submission-state"),
    )
    calls_before = list(client.calls)

    with pytest.raises(AcquisitionRefused, match="ledger.*(symlink|directory)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert client.calls == calls_before


def test_submission_state_transitions_reserved_attempted_consumed_with_o_excl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    exclusive_flags: list[int] = []
    original_open = os.open

    def tracking_open(
        path: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if flags & os.O_EXCL:
            exclusive_flags.append(flags)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(acquisition_module.os, "open", tracking_open)

    def assert_reserved() -> None:
        records = _submission_state_records()
        assert len(records) == 1
        assert _last_submission_state(records[0])["state"] == "reserved"

    def assert_attempted() -> None:
        record = _last_submission_state(_submission_state_records()[0])
        assert record["state"] == "attempted"

    client.requote_hook = assert_reserved
    client.submit_hook = assert_attempted

    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    records = _submission_state_records()
    assert len(records) == 1
    journal = _submission_journal(records[0])
    assert [item["state"] for item in journal] == [
        "reserved",
        "attempted",
        "consumed",
    ]
    assert [item["state_sequence"] for item in journal] == [1, 2, 3]
    assert journal[0]["previous_state_sha256"] is None
    for previous, current in zip(journal[:-1], journal[1:], strict=True):
        assert (
            current["previous_state_sha256"]
            == hashlib.sha256(
                json.dumps(previous, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                ),
            ).hexdigest()
        )
    record = _last_submission_state(records[0])
    assert record["state"] == "consumed"
    assert record["submission_identity_sha256"] == request.submission_identity()
    assert record["submission"] == request.submission()
    assert record["job_id"] == "XNAS-TEST-JOB"
    assert receipt["submission_identity_sha256"] == request.submission_identity()
    assert exclusive_flags and all(flags & os.O_EXCL for flags in exclusive_flags)
    state_root = Path(os.environ["DATABENTO_INGEST_STATE_DIR"])
    assert stat.S_IMODE(state_root.stat().st_mode) == 0o700
    assert stat.S_IMODE((state_root / "submissions").stat().st_mode) == 0o700
    assert stat.S_IMODE(records[0].stat().st_mode) == 0o600
    locks = list((state_root / "submissions").glob("*.lock"))
    assert len(locks) == 1
    assert stat.S_IMODE(locks[0].stat().st_mode) == 0o600
    assert locks[0].stat().st_size == 0
    assert "db-TEST-SECRET" not in json.dumps(record)


@pytest.mark.parametrize(
    ("target_state", "expected_submit_count"),
    [("reserved", 0), ("attempted", 0), ("consumed", 1)],
)
def test_partial_journal_transition_is_preserved_and_blocks_retry_before_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_state: str,
    expected_submit_count: int,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    original_write = os.write
    marker = f'"state":"{target_state}"'.encode("ascii")
    partial_fd: dict[str, int | None] = {"value": None}
    expected_record_path = (
        Path(os.environ["DATABENTO_INGEST_STATE_DIR"])
        / "submissions"
        / f"{request.submission_identity()}.json"
    )

    def partial_transition_write(fd: int, data: object) -> int:
        payload = bytes(data)  # type: ignore[arg-type]
        if partial_fd["value"] == fd:
            raise OSError(f"injected {target_state} journal write failure")
        status = os.fstat(fd)
        try:
            expected_status = expected_record_path.stat(follow_symlinks=False)
        except FileNotFoundError:
            expected_status = None
        if (
            marker in payload
            and stat.S_ISREG(status.st_mode)
            and stat.S_IMODE(status.st_mode) == 0o600
            and expected_status is not None
            and (status.st_dev, status.st_ino)
            == (expected_status.st_dev, expected_status.st_ino)
        ):
            partial_fd["value"] = fd
            return original_write(fd, payload[: max(1, len(payload) // 2)])
        return original_write(fd, payload)

    monkeypatch.setattr(acquisition_module.os, "write", partial_transition_write)

    with pytest.raises(OSError, match=f"injected {target_state}"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert partial_fd["value"] is not None
    assert [name for name, _ in client.calls].count("submit_job") == (
        expected_submit_count
    )
    record_path = _submission_state_records()[0]
    corrupt_payload = record_path.read_bytes()
    assert corrupt_payload
    assert not corrupt_payload.endswith(b"\n")
    blocked_client = _FakeClient(costs=(0.30,))
    blocked_factory_calls: list[str] = []

    def blocked_factory(api_key: str) -> _FakeClient:
        blocked_factory_calls.append(api_key)
        return blocked_client

    with pytest.raises(AcquisitionRefused, match="ledger.*corrupt"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=blocked_factory,
            clock=lambda: NOW + timedelta(minutes=2),
        )

    assert record_path.read_bytes() == corrupt_payload
    assert blocked_factory_calls == []
    assert blocked_client.calls == []


def test_corrupt_reserved_record_is_preserved_and_blocks_provider_submission(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)

    def corrupt_reserved_record() -> None:
        record_path = _submission_state_records()[0]
        record_path.write_text("corrupt\n", encoding="utf-8")

    client.requote_hook = corrupt_reserved_record

    with pytest.raises(AcquisitionRefused, match="ledger.*(corrupt|changed)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0
    assert _submission_state_records()[0].read_text(encoding="utf-8") == "corrupt\n"


def test_transition_race_preserves_unexpected_replacement_and_never_submits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    original_read = acquisition_module._read_submission_record
    reserved_reads = 0
    intruder: dict[str, object] = {}

    def inject_after_final_comparison(ledger: object) -> dict[str, object]:
        nonlocal reserved_reads, intruder
        record = original_read(ledger)
        if record.get("state") == "reserved":
            reserved_reads += 1
            if reserved_reads == 2:
                intruder = {
                    **record,
                    "state": "consumed",
                    "job_id": "UNEXPECTED-OTHER-WRITER",
                }
                _replace_with_intruder(_submission_state_records()[0], intruder)
        return record

    monkeypatch.setattr(
        acquisition_module,
        "_read_submission_record",
        inject_after_final_comparison,
    )

    with pytest.raises(AcquisitionRefused, match="ledger.*(changed|binding)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0
    assert _last_submission_state(_submission_state_records()[0]) == intruder
    blocked_client = _FakeClient(costs=(0.30,))
    with pytest.raises(AcquisitionRefused, match="submission (identity|ledger)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: blocked_client,
            clock=lambda: NOW + timedelta(minutes=1),
        )
    assert blocked_client.calls == []


def test_release_race_preserves_unexpected_replacement_and_blocks_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 1.01))
    request, plan, client = _preflight(tmp_path, client=client)
    original_read = acquisition_module._read_submission_record
    injected = False
    intruder: dict[str, object] = {}

    def inject_after_release_comparison(ledger: object) -> dict[str, object]:
        nonlocal injected, intruder
        record = original_read(ledger)
        if record.get("state") == "reserved" and not injected:
            injected = True
            intruder = {
                **record,
                "state": "attempted",
                "attempted_at": "2026-07-22T12:01:00Z",
            }
            _replace_with_intruder(_submission_state_records()[0], intruder)
        return record

    monkeypatch.setattr(
        acquisition_module,
        "_read_submission_record",
        inject_after_release_comparison,
    )

    with pytest.raises(AcquisitionRefused, match="re-quote.*cap"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0
    assert _last_submission_state(_submission_state_records()[0]) == intruder
    blocked_client = _FakeClient(costs=(0.30,))
    with pytest.raises(AcquisitionRefused, match="submission (identity|ledger)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: blocked_client,
            clock=lambda: NOW + timedelta(minutes=1),
        )
    assert blocked_client.calls == []


@pytest.mark.parametrize("tamper", ["wrong_mode", "symlink"])
def test_tampered_reserved_record_is_preserved_and_never_submitted(
    tmp_path: Path,
    tamper: str,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    held_target = tmp_path / "held-target.json"
    held_target.write_text("held\n", encoding="utf-8")

    def tamper_with_reserved_record() -> None:
        record_path = _submission_state_records()[0]
        if tamper == "wrong_mode":
            record_path.chmod(0o644)
        else:
            record_path.unlink()
            record_path.symlink_to(held_target)

    client.requote_hook = tamper_with_reserved_record

    with pytest.raises(AcquisitionRefused, match="ledger.*(mode|symlink|record)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0
    record_path = _submission_state_records()[0]
    if tamper == "wrong_mode":
        assert stat.S_IMODE(record_path.stat().st_mode) == 0o644
    else:
        assert record_path.is_symlink()
        assert held_target.read_text(encoding="utf-8") == "held\n"


def test_state_root_swap_after_attempt_is_detected_before_provider_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    state_root = Path(os.environ["DATABENTO_INGEST_STATE_DIR"])
    moved_root = tmp_path / "moved-submission-state"
    original_replace = acquisition_module._replace_submission_state

    def persist_then_swap(
        ledger: object,
        expected: dict[str, object],
        replacement: dict[str, object],
    ) -> None:
        original_replace(ledger, expected, replacement)
        if replacement["state"] == "attempted":
            state_root.rename(moved_root)
            state_root.mkdir(mode=0o700)
            state_root.chmod(0o700)
            (state_root / "submissions").mkdir(mode=0o700)
            (state_root / "submissions").chmod(0o700)

    monkeypatch.setattr(
        acquisition_module,
        "_replace_submission_state",
        persist_then_swap,
    )

    with pytest.raises(AcquisitionRefused, match="ledger.*(changed|binding)"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert [name for name, _ in client.calls].count("submit_job") == 0
    moved_records = list((moved_root / "submissions").glob("*.json"))
    assert len(moved_records) == 1
    assert _last_submission_state(moved_records[0])["state"] == "attempted"


def test_override_roots_are_explicitly_separate_local_idempotency_domains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path)
    first_client = _FakeClient(costs=(0.25, 0.30))
    first_plan = preflight_request(
        "db-TEST-SECRET",
        request,
        client_factory=lambda key: first_client,
        now=NOW,
    )
    first_root = tmp_path / "domain-one"
    monkeypatch.setenv("DATABENTO_INGEST_STATE_DIR", str(first_root))
    submit_planned_request(
        "db-TEST-SECRET",
        request,
        first_plan,
        client_factory=lambda key: first_client,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    second_client = _FakeClient(costs=(0.25, 0.30))
    second_plan = preflight_request(
        "db-TEST-SECRET",
        request,
        client_factory=lambda key: second_client,
        now=NOW,
    )
    second_root = tmp_path / "domain-two"
    monkeypatch.setenv("DATABENTO_INGEST_STATE_DIR", str(second_root))
    submit_planned_request(
        "db-TEST-SECRET",
        request,
        second_plan,
        client_factory=lambda key: second_client,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    assert [name for name, _ in first_client.calls].count("submit_job") == 1
    assert [name for name, _ in second_client.calls].count("submit_job") == 1
    assert len(list((first_root / "submissions").glob("*.json"))) == 1
    assert len(list((second_root / "submissions").glob("*.json"))) == 1


def test_same_submission_function_called_twice_refuses_before_second_provider_call(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    calls_after_first = list(client.calls)

    with pytest.raises(AcquisitionRefused, match="submission identity"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    assert client.calls == calls_after_first
    assert [name for name, _ in client.calls].count("submit_job") == 1


def test_concurrent_same_identity_allows_exactly_one_submission(
    tmp_path: Path,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)

    def submit_once() -> str:
        try:
            submit_planned_request(
                "db-TEST-SECRET",
                request,
                plan,
                client_factory=lambda key: client,
                clock=lambda: NOW + timedelta(minutes=1),
            )
        except AcquisitionRefused:
            return "refused"
        return "submitted"

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = sorted(executor.map(lambda _: submit_once(), range(2)))

    assert outcomes == ["refused", "submitted"]
    assert [name for name, _ in client.calls].count("submit_job") == 1


def test_requote_refusal_releases_reservation_for_safe_retry(
    tmp_path: Path,
) -> None:
    refusing_client = _FakeClient(costs=(0.25, 1.01))
    request, plan, refusing_client = _preflight(tmp_path, client=refusing_client)

    with pytest.raises(AcquisitionRefused, match="re-quote.*cap"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: refusing_client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    records = _submission_state_records()
    assert len(records) == 1
    assert _last_submission_state(records[0])["state"] == "released"
    retry_client = _FakeClient(costs=(0.30,))
    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: retry_client,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    assert receipt["job_id"] == "XNAS-TEST-JOB"
    journal = _submission_journal(_submission_state_records()[0])
    assert [item["state"] for item in journal] == [
        "reserved",
        "released",
        "reserved",
        "attempted",
        "consumed",
    ]
    assert [item["attempt"] for item in journal] == [1, 1, 2, 2, 2]
    assert _last_submission_state(_submission_state_records()[0])["state"] == "consumed"


def test_repeated_releases_exhaust_capacity_without_mutation_or_provider_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, plan, _ = _preflight(tmp_path)
    monkeypatch.setattr(
        acquisition_module,
        "MAX_SUBMISSION_JOURNAL_BYTES",
        5_000,
    )
    factory_calls: list[str] = []
    all_clients: list[_FakeClient] = []
    exhausted = False

    for _ in range(20):
        client = _FakeClient(costs=(1.01,))
        all_clients.append(client)
        before_factory_count = len(factory_calls)
        records = _submission_state_records()
        before_payload = records[0].read_bytes() if records else None

        def client_factory(api_key: str, selected: _FakeClient = client) -> _FakeClient:
            factory_calls.append(api_key)
            return selected

        try:
            submit_planned_request(
                "db-TEST-SECRET",
                request,
                plan,
                client_factory=client_factory,
                clock=lambda: NOW + timedelta(minutes=1),
            )
        except AcquisitionRefused as exc:
            if "capacity exhausted" not in str(exc):
                assert "re-quote exceeds" in str(exc)
                continue
            exhausted = True
            assert len(factory_calls) == before_factory_count
            record_path = _submission_state_records()[0]
            assert record_path.read_bytes() == before_payload
            assert _last_submission_state(record_path)["state"] == "released"
            break

    assert exhausted, "bounded journal must deterministically stop released retries"
    assert all(
        [name for name, _ in client.calls].count("submit_job") == 0
        for client in all_clients
    )


def test_initial_reservation_refuses_before_durable_state_when_terminal_path_will_not_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, plan, _ = _preflight(tmp_path)
    moment = NOW.replace(microsecond=123456)
    initial = {
        "schema_version": acquisition_module.SUBMISSION_STATE_SCHEMA_VERSION,
        "submission_identity_sha256": request.submission_identity(),
        "submission": request.submission(),
        "state": "reserved",
        "reserved_at": acquisition_module._utc_text(moment),
        "attempt": 1,
        "state_sequence": 1,
        "previous_state_sha256": None,
    }
    required = acquisition_module._reservation_path_capacity_size(initial)
    monkeypatch.setattr(
        acquisition_module,
        "MAX_SUBMISSION_JOURNAL_BYTES",
        required - 1,
    )
    factory_calls: list[str] = []

    with pytest.raises(AcquisitionRefused, match="capacity exhausted"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: factory_calls.append(key),
            clock=lambda: moment,
        )

    assert factory_calls == []
    assert _submission_state_records() == []


def test_exact_reserved_attempted_consumed_capacity_accepts_maximum_job_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    client.submit_response = {"id": "\x00" * 256, "state": "queued"}
    request, plan, client = _preflight(tmp_path, client=client)
    moment = NOW.replace(microsecond=123456)
    initial = {
        "schema_version": acquisition_module.SUBMISSION_STATE_SCHEMA_VERSION,
        "submission_identity_sha256": request.submission_identity(),
        "submission": request.submission(),
        "state": "reserved",
        "reserved_at": acquisition_module._utc_text(moment),
        "attempt": 1,
        "state_sequence": 1,
        "previous_state_sha256": None,
    }
    required = acquisition_module._reservation_path_capacity_size(initial)
    monkeypatch.setattr(
        acquisition_module,
        "MAX_SUBMISSION_JOURNAL_BYTES",
        required,
    )

    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: moment,
    )

    record_path = _submission_state_records()[0]
    assert record_path.stat().st_size == required
    assert receipt["job_id"] == "\x00" * 256
    assert _last_submission_state(record_path)["state"] == "consumed"


def test_attempt_requires_capacity_for_release_and_both_terminal_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(costs=(0.25, 0.30))
    request, plan, client = _preflight(tmp_path, client=client)
    boundary = {"value": 0}

    def shrink_capacity_after_reservation() -> None:
        record_path = _submission_state_records()[0]
        reserved = _last_submission_state(record_path)
        released = {
            **reserved,
            "state": "released",
            "released_at": "2026-07-22T12:01:00Z",
            "release_reason": "AcquisitionRefused",
            "state_sequence": int(reserved["state_sequence"]) + 1,
            "previous_state_sha256": acquisition_module._state_record_sha256(
                reserved,
            ),
        }
        boundary["value"] = record_path.stat().st_size + len(
            acquisition_module._canonical_json(released) + b"\n"
        )
        monkeypatch.setattr(
            acquisition_module,
            "MAX_SUBMISSION_JOURNAL_BYTES",
            boundary["value"],
        )

    client.requote_hook = shrink_capacity_after_reservation

    with pytest.raises(AcquisitionRefused, match="capacity exhausted"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    record_path = _submission_state_records()[0]
    assert record_path.stat().st_size == boundary["value"]
    assert _last_submission_state(record_path)["state"] == "released"
    assert [name for name, _ in client.calls].count("submit_job") == 0


def test_valid_orphaned_reservation_is_recovered_under_lock_before_retry(
    tmp_path: Path,
) -> None:
    request, plan, _ = _preflight(tmp_path)
    submission = request.submission()
    ledger, reserved = acquisition_module._reserve_submission_identity(
        request.submission_identity(),
        submission,
        NOW + timedelta(minutes=1),
    )
    ledger.close()
    client = _FakeClient(costs=(0.30,))

    receipt = submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: client,
        clock=lambda: NOW + timedelta(minutes=2),
    )

    assert receipt["job_id"] == "XNAS-TEST-JOB"
    journal = _submission_journal(_submission_state_records()[0])
    assert [record["state"] for record in journal] == [
        "reserved",
        "released",
        "reserved",
        "attempted",
        "consumed",
    ]
    assert journal[1]["release_reason"] == "recovered_orphaned_reservation"
    assert journal[0] == reserved
    assert [name for name, _ in client.calls].count("submit_job") == 1


def test_spawned_orphan_recovery_serializes_a_live_competing_process(
    tmp_path: Path,
) -> None:
    request_path = _write_request(tmp_path / "spawn-request.toml")
    request = BatchRequest.from_toml(request_path)
    request, plan, _ = _preflight(tmp_path, request=request)
    plan_path = tmp_path / "spawn.plan.json"
    write_canonical_json_no_clobber(plan_path, plan.to_dict())
    ledger, _ = acquisition_module._reserve_submission_identity(
        request.submission_identity(),
        request.submission(),
        NOW + timedelta(minutes=1),
    )
    ledger.close()
    state_root = os.environ["DATABENTO_INGEST_STATE_DIR"]
    context = multiprocessing.get_context("spawn")
    winner_start = context.Event()
    loser_start = context.Event()
    requote_ready = context.Event()
    requote_release = context.Event()
    result_queue = context.Queue()
    winner = context.Process(
        target=_spawn_submit_worker,
        args=(
            "winner",
            str(request_path),
            str(plan_path),
            state_root,
            winner_start,
            requote_ready,
            requote_release,
            result_queue,
            True,
        ),
    )
    loser = context.Process(
        target=_spawn_submit_worker,
        args=(
            "loser",
            str(request_path),
            str(plan_path),
            state_root,
            loser_start,
            requote_ready,
            requote_release,
            result_queue,
            False,
        ),
    )
    winner.start()
    loser.start()
    try:
        winner_start.set()
        assert requote_ready.wait(15), "winner did not reach held re-quote"
        loser_start.set()
        loser_result = result_queue.get(timeout=15)
        assert loser_result == {
            "tag": "loser",
            "status": "refused",
            "exception_type": "AcquisitionRefused",
            "message": (
                "submission identity is locked by another writer: "
                f"{request.submission_identity()}"
            ),
            "factory_calls": 0,
            "call_names": [],
            "submit_count": 0,
        }
        assert winner.is_alive(), "winner must still hold the identity lock"
        requote_release.set()
        winner_result = result_queue.get(timeout=15)
    finally:
        requote_release.set()
        _join_spawned_process(winner, "spawned winner")
        _join_spawned_process(loser, "spawned loser")
        result_queue.close()
        result_queue.join_thread()

    assert winner_result == {
        "tag": "winner",
        "status": "submitted",
        "job_id": "XNAS-TEST-JOB",
        "factory_calls": 1,
        "call_names": ["get_cost", "submit_job"],
        "submit_count": 1,
    }
    assert loser_result["submit_count"] + winner_result["submit_count"] == 1
    journal = _submission_journal(_submission_state_records()[0])
    assert [record["state"] for record in journal] == [
        "reserved",
        "released",
        "reserved",
        "attempted",
        "consumed",
    ]
    assert journal[1]["release_reason"] == "recovered_orphaned_reservation"


def test_spawned_process_death_after_reservation_is_recovered_by_later_process(
    tmp_path: Path,
) -> None:
    request_path = _write_request(tmp_path / "crash-request.toml")
    request = BatchRequest.from_toml(request_path)
    request, plan, _ = _preflight(tmp_path, request=request)
    plan_path = tmp_path / "crash.plan.json"
    write_canonical_json_no_clobber(plan_path, plan.to_dict())
    state_root = os.environ["DATABENTO_INGEST_STATE_DIR"]
    context = multiprocessing.get_context("spawn")
    durable_event = context.Event()
    status_value = context.Value("i", 0)
    reserving_process = context.Process(
        target=_spawn_reserve_then_exit,
        args=(str(request_path), state_root, durable_event, status_value),
    )

    reserving_process.start()
    assert durable_event.wait(15), "child did not persist its reservation"
    _join_spawned_process(reserving_process, "spawned reserving process")
    assert status_value.value == 1
    assert [
        record["state"]
        for record in _submission_journal(_submission_state_records()[0])
    ] == ["reserved"]

    start_event = context.Event()
    start_event.set()
    unused_ready_event = context.Event()
    unused_release_event = context.Event()
    result_queue = context.Queue()
    recovering_process = context.Process(
        target=_spawn_submit_worker,
        args=(
            "recovery",
            str(request_path),
            str(plan_path),
            state_root,
            start_event,
            unused_ready_event,
            unused_release_event,
            result_queue,
            False,
        ),
    )
    recovering_process.start()
    recovery_result = result_queue.get(timeout=15)
    _join_spawned_process(recovering_process, "spawned recovering process")
    result_queue.close()
    result_queue.join_thread()

    assert recovery_result == {
        "tag": "recovery",
        "status": "submitted",
        "job_id": "XNAS-TEST-JOB",
        "factory_calls": 1,
        "call_names": ["get_cost", "submit_job"],
        "submit_count": 1,
    }
    journal = _submission_journal(_submission_state_records()[0])
    assert [record["state"] for record in journal] == [
        "reserved",
        "released",
        "reserved",
        "attempted",
        "consumed",
    ]
    assert journal[1]["release_reason"] == "recovered_orphaned_reservation"


def test_submit_exception_preserves_ambiguous_attempt_and_blocks_retry(
    tmp_path: Path,
) -> None:
    failing_client = _FakeClient(costs=(0.25, 0.30))
    failing_client.submit_error = RuntimeError("ambiguous transport failure")
    request, plan, failing_client = _preflight(tmp_path, client=failing_client)

    with pytest.raises(RuntimeError, match="ambiguous transport failure"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: failing_client,
            clock=lambda: NOW + timedelta(minutes=1),
        )

    record_path = _submission_state_records()[0]
    assert _last_submission_state(record_path)["state"] == "attempted"
    retry_client = _FakeClient(costs=(0.30,))
    with pytest.raises(AcquisitionRefused, match="submission identity"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            plan,
            client_factory=lambda key: retry_client,
            clock=lambda: NOW + timedelta(minutes=1),
        )
    assert retry_client.calls == []
    assert _last_submission_state(record_path)["state"] == "attempted"


def test_fresh_preflight_for_identical_provider_request_remains_consumed(
    tmp_path: Path,
) -> None:
    first_client = _FakeClient(costs=(0.25, 0.30))
    request, plan, first_client = _preflight(tmp_path, client=first_client)
    submit_planned_request(
        "db-TEST-SECRET",
        request,
        plan,
        client_factory=lambda key: first_client,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    fresh_client = _FakeClient(costs=(0.25,))
    fresh_plan = preflight_request(
        "db-TEST-SECRET",
        request,
        client_factory=lambda key: fresh_client,
        now=NOW + timedelta(minutes=2),
    )
    blocked_client = _FakeClient(costs=(0.30,))

    with pytest.raises(AcquisitionRefused, match="submission identity"):
        submit_planned_request(
            "db-TEST-SECRET",
            request,
            fresh_plan,
            client_factory=lambda key: blocked_client,
            clock=lambda: NOW + timedelta(minutes=3),
        )

    assert blocked_client.calls == []


def test_cli_copied_plan_is_blocked_by_fixed_identity_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = _write_request(tmp_path / "request.toml")
    request, plan, _ = _preflight(
        tmp_path,
        request=BatchRequest.from_toml(request_path),
    )
    first_plan_path = tmp_path / "first.plan.json"
    copied_plan_path = tmp_path / "copied.plan.json"
    write_canonical_json_no_clobber(first_plan_path, plan.to_dict())
    write_canonical_json_no_clobber(copied_plan_path, plan.to_dict())
    submit_client = _FakeClient(costs=(0.30,))
    monkeypatch.setenv("DATABENTO_API_KEY", "db-ENV-SECRET")
    monkeypatch.setattr(
        acquisition_module,
        "_new_client",
        lambda api_key, client_factory: (submit_client, submit_client.sdk_version),
    )

    class _Clock(datetime):
        @classmethod
        def now(cls, tz: object) -> datetime:
            assert tz is timezone.utc
            return NOW + timedelta(minutes=1)

    monkeypatch.setattr(acquisition_module, "datetime", _Clock)

    cli.cmd_batch_submit(
        argparse.Namespace(config=str(request_path), plan=str(first_plan_path)),
    )
    with pytest.raises(SystemExit):
        cli.cmd_batch_submit(
            argparse.Namespace(config=str(request_path), plan=str(copied_plan_path)),
        )

    assert [name for name, _ in submit_client.calls].count("submit_job") == 1
    assert first_plan_path.with_name("first.plan.receipt.json").is_file()
    assert not copied_plan_path.with_name("copied.plan.receipt.json").exists()


def test_statistics_request_roster_matches_sealed_provider_metadata() -> None:
    config_path = (
        Path(__file__).parents[1]
        / "configs/requests/xnas_itch_regime86_statistics_20250203_20260710.toml"
    )
    request = BatchRequest.from_toml(config_path)

    assert request.dataset == "XNAS.ITCH"
    assert request.schema == "statistics"
    assert request.start == "2025-02-03"
    assert request.end == "2026-07-11"
    assert request.symbol_count == 86
    assert request.max_cost_usd == Decimal("1.00")
    assert request.symbol_source_sha256 == (
        "e127c9152df6bdc23a48b38bad9248b0ed196572e99b3e66b84934a406e8ac1c"
    )

    source_path = Path(request.symbol_source_path)
    if source_path.is_file():
        source_bytes = source_path.read_bytes()
        assert hashlib.sha256(source_bytes).hexdigest() == request.symbol_source_sha256
        source = json.loads(source_bytes)
        assert request.symbols == tuple(source["query"]["symbols"])
    else:
        assert len(request.symbols) == request.symbol_count


def test_plan_write_is_canonical_atomic_and_no_clobber(tmp_path: Path) -> None:
    _, plan, _ = _preflight(tmp_path)
    output = tmp_path / "plan.json"

    write_canonical_json_no_clobber(output, plan.to_dict())

    assert output.read_bytes() == plan.canonical_json() + b"\n"
    assert load_plan(output) == plan
    with pytest.raises(FileExistsError):
        write_canonical_json_no_clobber(output, {"replacement": True})
    assert output.read_bytes() == plan.canonical_json() + b"\n"


def test_plan_parser_rejects_unknown_secret_field_even_with_valid_known_hash(
    tmp_path: Path,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    injected = plan.to_dict()
    injected["api_key"] = "db-INJECTED-SECRET"

    with pytest.raises(ValueError, match="unknown batch plan fields.*api_key"):
        BatchPlan.from_dict(injected)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda raw: raw["dataset_range"].__setitem__("api_key", "db-SECRET"),
            "dataset_range.*exactly",
        ),
        (
            lambda raw: raw["dataset_range"]["schema"]["statistics"].__setitem__(
                "token", "SECRET"
            ),
            "schema range.*exactly",
        ),
        (
            lambda raw: raw["dataset_conditions"][0].__setitem__(
                "api_key", "db-SECRET"
            ),
            "dataset condition.*field",
        ),
        (
            lambda raw: raw["provider_query"].__setitem__("credential", "SECRET"),
            "provider_query.*exactly",
        ),
    ],
)
def test_plan_parser_rejects_nested_unknown_or_secret_fields_with_valid_hash(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    raw = plan.to_dict()
    mutation(raw)
    _rehash_plan_payload(raw)

    with pytest.raises(ValueError, match=message):
        BatchPlan.from_dict(raw)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("condition", "unknown", "condition.*enum"),
        ("date", "2025-2-3", "condition.*date"),
        ("last_modified_date", 20250203, "last_modified_date"),
        ("last_modified_date", "2025-2-3", "last_modified_date"),
    ],
)
def test_plan_parser_rejects_invalid_condition_enums_types_and_dates(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    raw = plan.to_dict()
    raw["dataset_conditions"][0][field] = value
    _rehash_plan_payload(raw)

    with pytest.raises(ValueError, match=message):
        BatchPlan.from_dict(raw)


@pytest.mark.parametrize(
    ("path", "message"),
    [
        (("dataset_range", "schema"), "dataset_range.*exactly"),
        (
            ("dataset_range", "schema", "statistics", "end"),
            "schema range.*exactly",
        ),
        (("dataset_conditions", 0, "condition"), "dataset condition.*field"),
    ],
)
def test_plan_parser_rejects_missing_nested_required_fields(
    tmp_path: Path,
    path: tuple[object, ...],
    message: str,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    raw = plan.to_dict()
    parent: object = raw
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    _rehash_plan_payload(raw)

    with pytest.raises(ValueError, match=message):
        BatchPlan.from_dict(raw)


def test_plan_parser_rejects_naive_non_date_range_timestamp(
    tmp_path: Path,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    raw = plan.to_dict()
    raw["dataset_range"]["schema"]["statistics"]["start"] = "2018-05-01T12:00:00"
    _rehash_plan_payload(raw)

    with pytest.raises(ValueError, match="timezone-aware"):
        BatchPlan.from_dict(raw)


def test_plan_accepts_official_condition_shape_with_optional_last_modified_date(
    tmp_path: Path,
) -> None:
    conditions = _complete_conditions()
    conditions[0] = {
        "date": "2025-02-03",
        "condition": "degraded",
        "last_modified_date": "2025-02-05",
    }
    conditions[1] = {
        "date": "2025-02-04",
        "condition": "available",
        "last_modified_date": None,
    }
    client = _FakeClient(conditions=tuple(conditions))

    _, plan, _ = _preflight(tmp_path, client=client)

    assert plan.to_dict()["dataset_conditions"] == client.conditions


def test_plan_internals_are_recursively_immutable_and_to_dict_is_defensive(
    tmp_path: Path,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    canonical_before = plan.canonical_json()

    with pytest.raises(TypeError):
        plan.provider_query["dataset"] = "MUTATED"
    with pytest.raises(TypeError):
        plan.dataset_range["schema"]["statistics"]["start"] = "MUTATED"
    with pytest.raises(TypeError):
        plan.dataset_conditions[0]["condition"] = "missing"

    thawed = plan.to_dict()
    thawed["provider_query"]["symbols"].append("MUTATED")
    thawed["dataset_range"]["schema"]["statistics"]["start"] = "MUTATED"
    thawed["dataset_conditions"][0]["condition"] = "missing"

    assert plan.canonical_json() == canonical_before
    assert "MUTATED" not in json.dumps(plan.to_dict())


def test_plan_create_defensively_copies_nested_provider_metadata(
    tmp_path: Path,
) -> None:
    request, original, _ = _preflight(tmp_path)
    dataset_range = original.to_dict()["dataset_range"]
    conditions = original.to_dict()["dataset_conditions"]
    schemas = list(original.available_schemas)

    plan = BatchPlan.create(
        request=request,
        estimated_at=original.estimated_at,
        sdk_version=original.sdk_version,
        dataset_range=dataset_range,
        available_schemas=schemas,
        dataset_conditions=conditions,
        estimated_cost_usd=original.estimated_cost_usd,
        estimated_record_count=original.estimated_record_count,
        estimated_billable_bytes=original.estimated_billable_bytes,
        availability_reasons=list(original.availability_reasons),
    )
    dataset_range["schema"]["statistics"]["start"] = "MUTATED"
    conditions[0]["condition"] = "missing"
    schemas.append("MUTATED")

    serialized = json.dumps(plan.to_dict())
    assert "MUTATED" not in serialized
    assert "missing" not in serialized


@pytest.mark.parametrize("invalid_source", ["dataset_range", "condition"])
def test_plan_create_rejects_nested_unknown_provider_metadata_fields(
    tmp_path: Path,
    invalid_source: str,
) -> None:
    dataset_range: dict[str, object] | None = None
    conditions: tuple[dict[str, object], ...] | None = None
    if invalid_source == "dataset_range":
        dataset_range = {
            "start": "2018-05-01",
            "end": "2026-08-01",
            "schema": {
                "statistics": {
                    "start": "2018-05-01",
                    "end": "2026-08-01",
                },
            },
            "api_key": "db-SECRET",
        }
    else:
        conditions = (
            {
                "date": "2025-02-03",
                "condition": "available",
                "token": "SECRET",
            },
        )
    client = _FakeClient(dataset_range=dataset_range, conditions=conditions)

    with pytest.raises(ValueError, match="exactly|field"):
        _preflight(tmp_path, client=client)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("schema_version", 1),
        ("request_fingerprint", 123),
        ("estimated_at", 123),
        ("sdk_version", 81),
        ("provider_query", []),
        ("dataset_range", []),
        ("available_schemas", "statistics"),
        ("dataset_conditions", {}),
        ("estimated_cost_usd", 0.25),
        ("estimated_record_count", 123.0),
        ("estimated_billable_bytes", True),
        ("max_cost_usd", 1.0),
        ("availability_ok", "true"),
        ("availability_reasons", "none"),
        ("plan_sha256", 123),
    ],
)
def test_plan_parser_rejects_inexact_top_level_field_types(
    tmp_path: Path,
    field: str,
    invalid_value: object,
) -> None:
    _, plan, _ = _preflight(tmp_path)
    invalid = plan.to_dict()
    invalid[field] = invalid_value

    with pytest.raises(ValueError, match=field):
        BatchPlan.from_dict(invalid)


def test_safe_batch_parsers_expose_no_api_key_argument() -> None:
    parser = cli.build_parser()

    plan_args = parser.parse_args(
        ["batch-plan", "--config", "request.toml", "--output", "plan.json"],
    )
    submit_args = parser.parse_args(
        ["batch-submit", "--config", "request.toml", "--plan", "plan.json"],
    )

    assert not hasattr(plan_args, "api_key")
    assert not hasattr(submit_args, "api_key")


def test_batch_plan_cli_uses_environment_key_and_writes_secret_free_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = _write_request(tmp_path / "request.toml")
    output_path = tmp_path / "plan.json"
    request = BatchRequest.from_toml(request_path)
    _, expected_plan, _ = _preflight(tmp_path, request=request)
    seen: dict[str, object] = {}

    def fake_preflight(api_key: str, actual_request: BatchRequest) -> BatchPlan:
        seen["api_key"] = api_key
        seen["request"] = actual_request
        return expected_plan

    monkeypatch.setenv("DATABENTO_API_KEY", "db-ENV-SECRET")
    monkeypatch.setattr(cli, "preflight_request", fake_preflight)
    monkeypatch.setattr(
        cli,
        "load_credentials",
        lambda: pytest.fail("credential file must not be read when env key is present"),
    )

    cli.cmd_batch_plan(
        argparse.Namespace(config=str(request_path), output=str(output_path)),
    )

    assert seen == {"api_key": "db-ENV-SECRET", "request": request}
    assert "db-ENV-SECRET" not in output_path.read_text(encoding="utf-8")
    assert load_plan(output_path) == expected_plan


def test_batch_submit_cli_writes_derived_secret_free_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = _write_request(tmp_path / "request.toml")
    request, plan, _ = _preflight(
        tmp_path, request=BatchRequest.from_toml(request_path)
    )
    plan_path = tmp_path / "statistics-plan.json"
    write_canonical_json_no_clobber(plan_path, plan.to_dict())
    expected_receipt = {
        "schema_version": "1.0",
        "job_id": "XNAS-TEST-JOB",
        "receipt_sha256": "b" * 64,
    }
    seen: dict[str, object] = {}

    def fake_submit(
        api_key: str,
        actual_request: BatchRequest,
        actual_plan: BatchPlan,
    ) -> dict[str, object]:
        seen["api_key"] = api_key
        seen["request"] = actual_request
        seen["plan"] = actual_plan
        return expected_receipt

    monkeypatch.setenv("DATABENTO_API_KEY", "db-ENV-SECRET")
    monkeypatch.setattr(cli, "submit_planned_request", fake_submit)
    monkeypatch.setattr(
        cli,
        "load_credentials",
        lambda: pytest.fail("credential file must not be read when env key is present"),
    )

    cli.cmd_batch_submit(
        argparse.Namespace(config=str(request_path), plan=str(plan_path)),
    )

    receipt_path = tmp_path / "statistics-plan.receipt.json"
    assert seen == {"api_key": "db-ENV-SECRET", "request": request, "plan": plan}
    assert json.loads(receipt_path.read_text(encoding="utf-8")) == expected_receipt
    assert "db-ENV-SECRET" not in receipt_path.read_text(encoding="utf-8")


def test_batch_submit_cli_refuses_existing_receipt_before_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = _write_request(tmp_path / "request.toml")
    _, plan, _ = _preflight(tmp_path, request=BatchRequest.from_toml(request_path))
    plan_path = tmp_path / "statistics-plan.json"
    write_canonical_json_no_clobber(plan_path, plan.to_dict())
    receipt_path = tmp_path / "statistics-plan.receipt.json"
    receipt_path.write_text('{"existing":true}\n', encoding="utf-8")

    monkeypatch.setenv("DATABENTO_API_KEY", "db-ENV-SECRET")
    monkeypatch.setattr(
        cli,
        "submit_planned_request",
        lambda *args, **kwargs: pytest.fail("must refuse before provider submission"),
    )

    with pytest.raises(SystemExit):
        cli.cmd_batch_submit(
            argparse.Namespace(config=str(request_path), plan=str(plan_path)),
        )

    assert receipt_path.read_text(encoding="utf-8") == '{"existing":true}\n'


def test_batch_module_exports_guarded_interfaces_and_pins_verified_sdk() -> None:
    assert batch.preflight_request is preflight_request
    assert batch.submit_planned_request is submit_planned_request

    pyproject_path = Path(__file__).parents[1] / "pyproject.toml"
    with pyproject_path.open("rb") as handle:
        pyproject = tomllib.load(handle)
    assert pyproject["project"]["optional-dependencies"]["api"] == [
        "databento>=0.81,<0.82",
    ]


def test_sdk_fakes_expose_exact_pinned_call_parameter_names() -> None:
    expected = {
        _FakeMetadata.get_dataset_range: ("self", "dataset"),
        _FakeMetadata.list_schemas: ("self", "dataset"),
        _FakeMetadata.get_dataset_condition: (
            "self",
            "dataset",
            "start_date",
            "end_date",
        ),
        _FakeMetadata.get_cost: (
            "self",
            "dataset",
            "start",
            "end",
            "mode",
            "symbols",
            "schema",
            "stype_in",
            "limit",
        ),
        _FakeMetadata.get_record_count: (
            "self",
            "dataset",
            "start",
            "end",
            "symbols",
            "schema",
            "stype_in",
            "limit",
        ),
        _FakeMetadata.get_billable_size: (
            "self",
            "dataset",
            "start",
            "end",
            "symbols",
            "schema",
            "stype_in",
            "limit",
        ),
        _FakeBatch.submit_job: (
            "self",
            "dataset",
            "symbols",
            "schema",
            "start",
            "end",
            "encoding",
            "compression",
            "pretty_px",
            "pretty_ts",
            "map_symbols",
            "split_symbols",
            "split_duration",
            "split_size",
            "delivery",
            "stype_in",
            "stype_out",
            "limit",
        ),
    }

    for method, parameter_names in expected.items():
        assert tuple(inspect.signature(method).parameters) == parameter_names


def test_installed_databento_081_signature_and_submit_response_contract() -> None:
    try:
        installed_version = importlib.metadata.version("databento")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("optional Databento SDK is not installed")

    assert installed_version.split(".")[:2] == ["0", "81"]
    from databento.historical.api.batch import BatchHttpAPI
    from databento.historical.api.metadata import MetadataHttpAPI

    expected = {
        MetadataHttpAPI.get_dataset_range: ("self", "dataset"),
        MetadataHttpAPI.list_schemas: ("self", "dataset"),
        MetadataHttpAPI.get_dataset_condition: (
            "self",
            "dataset",
            "start_date",
            "end_date",
        ),
        MetadataHttpAPI.get_cost: (
            "self",
            "dataset",
            "start",
            "end",
            "mode",
            "symbols",
            "schema",
            "stype_in",
            "limit",
        ),
        MetadataHttpAPI.get_record_count: (
            "self",
            "dataset",
            "start",
            "end",
            "symbols",
            "schema",
            "stype_in",
            "limit",
        ),
        MetadataHttpAPI.get_billable_size: (
            "self",
            "dataset",
            "start",
            "end",
            "symbols",
            "schema",
            "stype_in",
            "limit",
        ),
        BatchHttpAPI.submit_job: (
            "self",
            "dataset",
            "symbols",
            "schema",
            "start",
            "end",
            "encoding",
            "compression",
            "pretty_px",
            "pretty_ts",
            "map_symbols",
            "split_symbols",
            "split_duration",
            "split_size",
            "delivery",
            "stype_in",
            "stype_out",
            "limit",
        ),
    }
    for method, parameter_names in expected.items():
        assert tuple(inspect.signature(method).parameters) == parameter_names

    submit_return = inspect.signature(BatchHttpAPI.submit_job).return_annotation
    assert str(submit_return).replace("typing.", "") == "dict[str, Any]"
