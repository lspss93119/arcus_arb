"""Persistence analysis: event boundaries, durations, CLI compatibility."""

import csv
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import analyze  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DURATIONS = [0, 1, 2, 3, 5, 10]


def sample(timestamp_ms, sell_edge=0.0, buy_edge=0.0):
    return {
        "timestamp_ms": timestamp_ms,
        "premium_bps": 0.0,
        "sell_edge_bps": sell_edge,
        "buy_edge_bps": buy_edge,
    }


def counts(rows, *, midline=0.0, fees=0.0, band=1.0):
    result = analyze.count_persistence_events(
        rows,
        midline_bps=midline,
        fees_bps=fees,
        bands=[band],
        durations_sec=DURATIONS,
    )
    return result[band]


def test_continuous_threshold_crossing_is_one_event():
    result = counts(
        [
            sample(-1_000, sell_edge=0.9),
            sample(0, sell_edge=1.0),
            sample(1_000, sell_edge=1.1),
            sample(2_000, sell_edge=1.2),
        ]
    )

    assert result["sell"][0] == 1
    assert result["sell"][1] == 1
    assert result["sell"][2] == 1


def test_falling_below_threshold_resets_before_later_crossing():
    result = counts(
        [
            sample(0, sell_edge=1.0),
            sample(1_000, sell_edge=1.0),
            sample(2_000, sell_edge=0.9),
            sample(3_000, sell_edge=1.0),
        ]
    )

    assert result["sell"][0] == 2
    assert result["sell"][1] == 1


def test_sell_and_buy_events_are_independent():
    result = counts(
        [
            sample(0, sell_edge=1.0, buy_edge=0.0),
            sample(1_000, sell_edge=1.0, buy_edge=1.0),
            sample(2_000, sell_edge=0.0, buy_edge=1.0),
        ]
    )

    assert result["sell"][0] == 1
    assert result["sell"][1] == 1
    assert result["buy"][0] == 1
    assert result["buy"][1] == 1


def test_data_gap_over_1500ms_splits_qualifying_events():
    result = counts(
        [
            sample(0, sell_edge=1.0),
            sample(1_500, sell_edge=1.0),
            sample(3_001, sell_edge=1.0),
        ]
    )

    assert result["sell"][0] == 2
    assert result["sell"][1] == 1


def test_persistence_duration_counts_are_event_counts():
    rows = []
    cursor_sec = 0
    for duration_sec in (0, 1, 2, 3, 5, 10):
        for offset_sec in range(duration_sec + 1):
            rows.append(sample((cursor_sec + offset_sec) * 1_000, sell_edge=1.0))
        cursor_sec += duration_sec + 1
        rows.append(sample(cursor_sec * 1_000, sell_edge=0.0))
        cursor_sec += 1

    result = counts(rows)

    assert result["sell"] == {0: 6, 1: 5, 2: 4, 3: 3, 5: 2, 10: 1}


def test_persistence_conditions_use_midline_and_fees():
    result = counts(
        [
            sample(0, sell_edge=8.0, buy_edge=4.0),
        ],
        midline=2.0,
        fees=1.0,
        band=5.0,
    )

    assert result["sell"][0] == 1  # 8 - 2 - 1 == 5
    assert result["buy"][0] == 1  # 4 + 2 - 1 == 5


def write_minutes(path: Path) -> None:
    fields = [
        "minute_ts",
        "premium_close_bps",
        "premium_mean_bps",
        "sell_edge_max_bps",
        "buy_edge_max_bps",
        "samples",
    ]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for i in range(30):
            writer.writerow(
                {
                    "minute_ts": 1_700_000_000 + i * 60,
                    "premium_close_bps": 2.0,
                    "premium_mean_bps": 2.0,
                    "sell_edge_max_bps": 5.0,
                    "buy_edge_max_bps": 1.0,
                    "samples": 60,
                }
            )


def run_analyzer(minutes_path: Path, *extra: str):
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "analyze.py"),
            "--csv",
            str(minutes_path),
            "--min-samples",
            "1",
            *extra,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_analyzer_without_samples_keeps_existing_output():
    directory = Path(tempfile.mkdtemp())
    minutes_path = directory / "minutes.csv"
    write_minutes(minutes_path)

    result = run_analyzer(minutes_path)

    assert result.returncode == 0
    assert "minutes each band would have fired" in result.stdout
    assert "thresholds:" in result.stdout
    assert "sample-data coverage" not in result.stdout
    assert "persistence event counts" not in result.stdout.lower()


def test_sample_coverage_comes_from_samples_not_minutes():
    directory = Path(tempfile.mkdtemp())
    minutes_path = directory / "minutes.csv"
    samples_path = directory / "samples.csv"
    write_minutes(minutes_path)
    with samples_path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["timestamp_ms", "premium_bps", "sell_edge_bps", "buy_edge_bps"],
        )
        writer.writeheader()
        for offset_ms in (0, 1_000, 10_000, 11_000):
            writer.writerow(
                {
                    "timestamp_ms": 1_700_000_000_000 + offset_ms,
                    "premium_bps": 2.0,
                    "sell_edge_bps": 5.0,
                    "buy_edge_bps": 1.0,
                }
            )

    result = run_analyzer(minutes_path, "--samples", str(samples_path))

    assert result.returncode == 0
    assert "sample-data span: 11.0s" in result.stdout
    assert "observed coverage: 2.0s" in result.stdout
    assert "usable samples: 4" in result.stdout
    assert "independent of minute-data coverage" in result.stdout
    assert "gap rule: a gap > 1500 ms ends the current event" in result.stdout
    assert "persistence event counts (not minute counts)" in result.stdout.lower()
