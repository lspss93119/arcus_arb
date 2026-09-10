"""Non-destructive import of legacy market-history CSV files."""

from __future__ import annotations

import csv
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from entropy_arb.storage import (
    EntropyReferenceRow,
    HedgeReferenceRow,
    InsertCounts,
    MarketHistoryStore,
    MinuteRow,
    SampleRow,
)

MIGRATION_BATCH_ROWS = 5_000
SUPPORTED_HEDGES = ("lighter-rh", "tradexyz", "lighter")

SAMPLE_HEADER = [
    "timestamp_ms",
    "premium_bps",
    "sell_edge_bps",
    "buy_edge_bps",
    "entropy_bid",
    "entropy_ask",
    "hedge_bid",
    "hedge_ask",
    "entropy_book_update_ms",
    "hedge_book_update_ms",
]
MINUTE_HEADER = [
    "minute_ts",
    "time_utc",
    "symbol",
    "hedge",
    "entropy_bid",
    "entropy_ask",
    "hedge_bid",
    "hedge_ask",
    "premium_open_bps",
    "premium_high_bps",
    "premium_low_bps",
    "premium_close_bps",
    "premium_mean_bps",
    "premium_std_bps",
    "sell_edge_mean_bps",
    "sell_edge_max_bps",
    "buy_edge_mean_bps",
    "buy_edge_max_bps",
    "samples",
]
ENTROPY_REFERENCE_HEADER = ["recv_ms", "oracle_px", "mark_px"]
HEDGE_REFERENCE_HEADER = ["recv_ms", "server_ms", "index_px", "mark_px"]


@dataclass(frozen=True)
class MigrationFileReport:
    path: str
    dataset: str | None
    source_rows: int
    valid_rows: int
    invalid_rows: int
    already_existing: int
    conflicting_key_rows: int
    inserted_rows: int
    min_timestamp: int | None
    max_timestamp: int | None
    status: str
    message: str


@dataclass(frozen=True)
class _FileSpec:
    dataset: str
    header: list[str]
    parser: Callable[[dict[str, str], str, str], object]
    timestamp: Callable[[object], int]


def _sample_timestamp(row: object) -> int:
    if not isinstance(row, SampleRow):
        raise TypeError("sample parser returned an unexpected row type")
    return row.timestamp_ms


def _minute_timestamp(row: object) -> int:
    if not isinstance(row, MinuteRow):
        raise TypeError("minute parser returned an unexpected row type")
    return row.minute_ts


def _entropy_reference_timestamp(row: object) -> int:
    if not isinstance(row, EntropyReferenceRow):
        raise TypeError("entropy reference parser returned an unexpected row type")
    return row.recv_ms


def _hedge_reference_timestamp(row: object) -> int:
    if not isinstance(row, HedgeReferenceRow):
        raise TypeError("hedge reference parser returned an unexpected row type")
    return row.recv_ms


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError("must be positive")
    return parsed


def _finite(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("must be finite")
    return parsed


def _positive_float(value: str) -> float:
    parsed = _finite(value)
    if parsed <= 0:
        raise ValueError("must be positive")
    return parsed


def _symbol(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("symbol must be text")
    parsed = value.strip()
    if not parsed:
        raise ValueError("symbol must be non-empty")
    return parsed


def _hedge(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("hedge must be text")
    parsed = value.strip()
    if parsed not in SUPPORTED_HEDGES:
        raise ValueError("unsupported hedge")
    return parsed


def _sample_row(values: dict[str, str], symbol: str, hedge: str) -> SampleRow:
    return SampleRow(
        timestamp_ms=_positive_int(values["timestamp_ms"]),
        symbol=_symbol(symbol),
        hedge=_hedge(hedge),
        premium_bps=_finite(values["premium_bps"]),
        sell_edge_bps=_finite(values["sell_edge_bps"]),
        buy_edge_bps=_finite(values["buy_edge_bps"]),
        entropy_bid=_positive_float(values["entropy_bid"]),
        entropy_ask=_positive_float(values["entropy_ask"]),
        hedge_bid=_positive_float(values["hedge_bid"]),
        hedge_ask=_positive_float(values["hedge_ask"]),
        entropy_book_update_ms=_positive_int(values["entropy_book_update_ms"]),
        hedge_book_update_ms=_positive_int(values["hedge_book_update_ms"]),
    )


def _minute_row(values: dict[str, str], _: str, __: str) -> MinuteRow:
    return MinuteRow(
        minute_ts=_positive_int(values["minute_ts"]),
        symbol=_symbol(values["symbol"]),
        hedge=_hedge(values["hedge"]),
        entropy_bid=_positive_float(values["entropy_bid"]),
        entropy_ask=_positive_float(values["entropy_ask"]),
        hedge_bid=_positive_float(values["hedge_bid"]),
        hedge_ask=_positive_float(values["hedge_ask"]),
        premium_open_bps=_finite(values["premium_open_bps"]),
        premium_high_bps=_finite(values["premium_high_bps"]),
        premium_low_bps=_finite(values["premium_low_bps"]),
        premium_close_bps=_finite(values["premium_close_bps"]),
        premium_mean_bps=_finite(values["premium_mean_bps"]),
        premium_std_bps=_finite(values["premium_std_bps"]),
        sell_edge_mean_bps=_finite(values["sell_edge_mean_bps"]),
        sell_edge_max_bps=_finite(values["sell_edge_max_bps"]),
        buy_edge_mean_bps=_finite(values["buy_edge_mean_bps"]),
        buy_edge_max_bps=_finite(values["buy_edge_max_bps"]),
        samples=_positive_int(values["samples"]),
    )


def _entropy_reference_row(
    values: dict[str, str], symbol: str, hedge: str
) -> EntropyReferenceRow:
    return EntropyReferenceRow(
        symbol=_symbol(symbol),
        hedge=_hedge(hedge),
        recv_ms=_positive_int(values["recv_ms"]),
        oracle_px=_positive_float(values["oracle_px"]),
        mark_px=_positive_float(values["mark_px"]),
    )


def _hedge_reference_row(
    values: dict[str, str], symbol: str, hedge: str
) -> HedgeReferenceRow:
    return HedgeReferenceRow(
        symbol=_symbol(symbol),
        hedge=_hedge(hedge),
        recv_ms=_positive_int(values["recv_ms"]),
        server_ms=_positive_int(values["server_ms"]),
        index_px=_positive_float(values["index_px"]),
        mark_px=_positive_float(values["mark_px"]),
    )


_SPECS = {
    "samples": _FileSpec("samples", SAMPLE_HEADER, _sample_row, _sample_timestamp),
    "minutes": _FileSpec("minutes", MINUTE_HEADER, _minute_row, _minute_timestamp),
    "entropy_reference": _FileSpec(
        "entropy_reference",
        ENTROPY_REFERENCE_HEADER,
        _entropy_reference_row,
        _entropy_reference_timestamp,
    ),
    "hedge_reference": _FileSpec(
        "hedge_reference",
        HEDGE_REFERENCE_HEADER,
        _hedge_reference_row,
        _hedge_reference_timestamp,
    ),
}


def _report(
    path: Path,
    dataset: str | None,
    *,
    status: str,
    message: str,
    source_rows: int = 0,
    valid_rows: int = 0,
    invalid_rows: int = 0,
    already_existing: int = 0,
    conflicting_key_rows: int = 0,
    inserted_rows: int = 0,
    min_timestamp: int | None = None,
    max_timestamp: int | None = None,
) -> MigrationFileReport:
    return MigrationFileReport(
        path=str(path),
        dataset=dataset,
        source_rows=source_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        already_existing=already_existing,
        conflicting_key_rows=conflicting_key_rows,
        inserted_rows=inserted_rows,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
        status=status,
        message=message,
    )


def _candidate_dataset(path: Path) -> str | None:
    name = path.name
    if name.startswith("samples-v2") and name.endswith(".csv"):
        return "samples"
    if name.startswith("minutes") and name.endswith(".csv"):
        return "minutes"
    if name.startswith("reference-") and name.endswith(".csv"):
        if name.endswith("-entropy.csv"):
            return "entropy_reference"
        return "hedge_reference"
    return None


def _parse_filename_pair(stem: str, prefix: str) -> tuple[str, str] | None:
    if not stem.startswith(prefix):
        return None
    rest = stem[len(prefix) :]
    for hedge in SUPPORTED_HEDGES:
        suffix = f"-{hedge}"
        if rest.endswith(suffix):
            symbol = rest[: -len(suffix)].removeprefix("-")
            if symbol:
                try:
                    return _symbol(symbol), _hedge(hedge)
                except ValueError:
                    return None
    return None


def _sample_filename_pair(path: Path) -> tuple[str, str] | None:
    return _parse_filename_pair(path.stem, "samples-v2")


def _reference_filename_pair(path: Path, dataset: str) -> tuple[str, str] | None:
    stem = path.stem
    if dataset == "entropy_reference":
        if not stem.endswith("-entropy"):
            return None
        stem = stem[: -len("-entropy")]
    return _parse_filename_pair(stem, "reference-")


def _read_companion_pair(path: Path) -> tuple[str, str] | None:
    if not path.is_file():
        return None
    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames != MINUTE_HEADER:
                return None
            pairs: set[tuple[str, str]] = set()
            for row in reader:
                symbol = row.get("symbol")
                hedge = row.get("hedge")
                if not isinstance(symbol, str) or not isinstance(hedge, str):
                    return None
                pairs.add((symbol.strip(), hedge.strip()))
    except (OSError, UnicodeError, csv.Error, KeyError, TypeError, ValueError):
        return None
    if len(pairs) != 1:
        return None
    symbol, hedge = pairs.pop()
    try:
        return _symbol(symbol), _hedge(hedge)
    except ValueError:
        return None


def _sample_provenance(
    path: Path, mappings: dict[str, tuple[str, str]]
) -> tuple[str, str] | None:
    filename_pair = _sample_filename_pair(path)
    if filename_pair is not None:
        return filename_pair
    suffix = path.name[len("samples-v2") :]
    companion_pair = _read_companion_pair(path.with_name(f"minutes{suffix}"))
    if companion_pair is not None:
        return companion_pair
    mapping = mappings.get(path.name)
    if mapping is None:
        return None
    try:
        return _symbol(mapping[0]), _hedge(mapping[1])
    except (IndexError, ValueError):
        return None


def _add_counts(total: InsertCounts, new: InsertCounts) -> InsertCounts:
    return InsertCounts(
        inserted=total.inserted + new.inserted,
        duplicates=total.duplicates + new.duplicates,
        conflicts=total.conflicts + new.conflicts,
    )


def _import_batches(
    store: MarketHistoryStore, dataset: str, rows: Iterable[object]
) -> InsertCounts:
    total = InsertCounts()
    batch: list[object] = []
    for row in rows:
        batch.append(row)
        if len(batch) == MIGRATION_BATCH_ROWS:
            total = _add_counts(total, store.import_rows(dataset, batch))
            batch.clear()
    if batch:
        total = _add_counts(total, store.import_rows(dataset, batch))
    return total


def _migrate_file(
    path: Path,
    dataset: str,
    store: MarketHistoryStore,
    mappings: dict[str, tuple[str, str]],
) -> MigrationFileReport:
    spec = _SPECS[dataset]
    source_rows = 0
    valid_rows: list[object] = []
    invalid_rows = 0
    min_timestamp: int | None = None
    max_timestamp: int | None = None
    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames != spec.header:
                return _report(
                    path, dataset, status="FAIL", message="unsupported CSV header"
                )
            if dataset == "samples":
                provenance = _sample_provenance(path, mappings)
                if provenance is None:
                    return _report(
                        path,
                        dataset,
                        status="NEEDS_MAPPING",
                        message="sample provenance is unresolved",
                    )
            elif dataset in {"entropy_reference", "hedge_reference"}:
                provenance = _reference_filename_pair(path, dataset)
                if provenance is None:
                    return _report(
                        path,
                        dataset,
                        status="FAIL",
                        message="reference filename provenance is invalid",
                    )
            else:
                provenance = ("", "")
            for values in reader:
                source_rows += 1
                try:
                    row = spec.parser(values, *provenance)
                except (KeyError, TypeError, ValueError):
                    invalid_rows += 1
                    continue
                valid_rows.append(row)
                timestamp = spec.timestamp(row)
                min_timestamp = (
                    timestamp
                    if min_timestamp is None
                    else min(min_timestamp, timestamp)
                )
                max_timestamp = (
                    timestamp
                    if max_timestamp is None
                    else max(max_timestamp, timestamp)
                )
    except (OSError, UnicodeError, csv.Error) as exc:
        return _report(
            path,
            dataset,
            status="FAIL",
            message=f"unable to read CSV: {exc}",
            source_rows=source_rows,
            valid_rows=len(valid_rows),
            invalid_rows=invalid_rows,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )

    try:
        counts = _import_batches(store, dataset, valid_rows)
    except Exception as exc:
        return _report(
            path,
            dataset,
            status="FAIL",
            message=f"database import failed: {exc}",
            source_rows=source_rows,
            valid_rows=len(valid_rows),
            invalid_rows=invalid_rows,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )

    if not valid_rows and invalid_rows:
        status = "FAIL"
        message = "no valid rows could be imported"
    elif invalid_rows or counts.conflicts:
        status = "PARTIAL"
        message = "imported with invalid or conflicting rows"
    else:
        status = "PASS"
        message = "imported"
    return _report(
        path,
        dataset,
        status=status,
        message=message,
        source_rows=source_rows,
        valid_rows=len(valid_rows),
        invalid_rows=invalid_rows,
        already_existing=counts.duplicates,
        conflicting_key_rows=counts.conflicts,
        inserted_rows=counts.inserted,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )


def migrate_directory(
    source: Path, database: Path, mappings: dict[str, tuple[str, str]]
) -> list[MigrationFileReport]:
    """Import supported legacy CSV files without changing their source files."""
    source = Path(source)
    database = Path(database)
    if not source.is_dir():
        return [
            _report(
                source, None, status="FAIL", message="source directory does not exist"
            )
        ]

    candidates: list[tuple[Path, str]] = []
    for path in sorted(source.iterdir()):
        if not path.is_file():
            continue
        dataset = _candidate_dataset(path)
        if dataset is not None:
            candidates.append((path, dataset))
    try:
        store = MarketHistoryStore(database)
    except Exception as exc:
        if not candidates:
            return [
                _report(
                    database,
                    None,
                    status="FAIL",
                    message=f"database open failed: {exc}",
                )
            ]
        return [
            _report(
                path, dataset, status="FAIL", message=f"database open failed: {exc}"
            )
            for path, dataset in candidates
        ]

    try:
        reports = [
            _migrate_file(path, dataset, store, mappings)
            for path, dataset in candidates
        ]
        if any(report.status in {"PASS", "PARTIAL"} for report in reports):
            store.set_meta(
                "last_migration_at_utc", datetime.now(UTC).isoformat(timespec="seconds")
            )
        return reports
    finally:
        store.close()
