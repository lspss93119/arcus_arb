#!/usr/bin/env python3
"""Import supported legacy market-history CSV files into SQLite."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_arb.migration import (
    SUPPORTED_HEDGES,
    MigrationFileReport,
    migrate_directory,
)


def _mapping(value: str) -> tuple[str, tuple[str, str]]:
    try:
        filename, pair = value.split("=", 1)
        symbol, hedge = pair.split(",", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("mapping must be FILE=SYMBOL,HEDGE") from exc
    filename = filename.strip()
    symbol = symbol.strip()
    hedge = hedge.strip()
    if not filename or not symbol or hedge not in SUPPORTED_HEDGES:
        raise argparse.ArgumentTypeError(
            "mapping must name a symbol and supported hedge"
        )
    return filename, (symbol, hedge)


def _print_report(report: MigrationFileReport) -> None:
    print(
        f"{report.status:13} {Path(report.path).name} "
        f"source={report.source_rows} valid={report.valid_rows} invalid={report.invalid_rows} "
        f"inserted={report.inserted_rows} existing={report.already_existing} "
        f"conflicts={report.conflicting_key_rows} {report.message}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("logs"))
    parser.add_argument(
        "--database", type=Path, default=Path("data/market-history.sqlite")
    )
    parser.add_argument(
        "--map", dest="mappings", action="append", type=_mapping, default=[]
    )
    args = parser.parse_args(argv)
    mappings = dict(args.mappings)

    reports = migrate_directory(args.source, args.database, mappings)
    for report in reports:
        _print_report(report)
    statuses = {report.status for report in reports}
    print(
        f"summary files={len(reports)} pass={sum(r.status == 'PASS' for r in reports)} "
        f"partial={sum(r.status == 'PARTIAL' for r in reports)} "
        f"needs_mapping={sum(r.status == 'NEEDS_MAPPING' for r in reports)} "
        f"fail={sum(r.status == 'FAIL' for r in reports)}"
    )
    if "FAIL" in statuses:
        return 1
    if "NEEDS_MAPPING" in statuses:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
