#!/usr/bin/env python3
"""Create a consistent standalone market-history SQLite snapshot."""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_arb.snapshot import create_snapshot


def _default_output() -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%SZ")
    return Path("exports") / f"market-history-snapshot-{timestamp}.sqlite"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database", type=Path, default=Path("data/market-history.sqlite")
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    output = args.output or _default_output()

    try:
        snapshot = create_snapshot(args.database, output)
    except Exception as exc:
        print(f"snapshot failed: {exc}", file=sys.stderr)
        return 1

    print(f"snapshot source={args.database} output={snapshot} quick_check=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
