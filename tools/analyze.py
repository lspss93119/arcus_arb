#!/usr/bin/env python3
"""Legacy CSV ad-hoc analyzer for existing minute and sample exports.

This tool reads user-supplied legacy CSV files. The current live recorder and
reference collectors persist SQLite market history in `recorder.database`; this
tool does not query that database and does not diagnose, select, or switch a
live strategy.

For a legacy CSV, it prints:

  * the premium distribution (midline candidates),
  * how often each candidate upper/lower band would have fired,
  * optional second-level persistence event counts,
  * a legacy candidate-band summary for manual review.

此工具仅分析既有 CSV export，输出溢价分布和各档候选带宽的触发频率。当前
recorder/reference 会将市场历史写入 SQLite；本工具不会读取 SQLite，也不会自动
诊断市场、选择策略或切换策略。

Usage:
    python3 tools/analyze.py                    # legacy logs/minutes.csv
    python3 tools/analyze.py --csv path.csv --hours 24 --min-samples 10
    python3 tools/analyze.py --csv minutes.csv --samples samples.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
import time

CANDIDATES = [1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0]
PERSISTENCE_SECONDS = [0, 1, 2, 3, 5, 10]
MAX_SAMPLE_GAP_MS = 1_500


def pctl(sorted_vals: list, q: float) -> float:
    """Linear-interpolated percentile of a pre-sorted list, q in [0, 100]."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


def load_rows(path: str, hours: float, min_samples: int) -> list:
    cutoff = time.time() - hours * 3600 if hours > 0 else 0.0
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            try:
                if float(r["minute_ts"]) < cutoff:
                    continue
                if int(r["samples"]) < min_samples:
                    continue
                rows.append(
                    {
                        "ts": float(r["minute_ts"]),
                        "prem": float(r["premium_close_bps"]),
                        "prem_mean": float(r["premium_mean_bps"]),
                        "sell_max": float(r["sell_edge_max_bps"]),
                        "buy_max": float(r["buy_edge_max_bps"]),
                    }
                )
            except (KeyError, ValueError):
                continue
    return rows


def load_sample_rows(path: str, hours: float) -> list:
    cutoff_ms = (time.time() - hours * 3600) * 1000 if hours > 0 else 0.0
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            try:
                timestamp_ms = int(r["timestamp_ms"])
                if timestamp_ms < cutoff_ms:
                    continue
                rows.append(
                    {
                        "timestamp_ms": timestamp_ms,
                        "premium_bps": float(r["premium_bps"]),
                        "sell_edge_bps": float(r["sell_edge_bps"]),
                        "buy_edge_bps": float(r["buy_edge_bps"]),
                    }
                )
            except (KeyError, ValueError):
                continue
    return sorted(rows, key=lambda r: r["timestamp_ms"])


def _event_durations_ms(rows: list, qualifies, max_gap_ms: int) -> list[int]:
    durations = []
    start_ms = None
    last_qualifying_ms = None
    previous_ms = None

    for row in rows:
        timestamp_ms = row["timestamp_ms"]
        if previous_ms is not None and timestamp_ms - previous_ms > max_gap_ms:
            if start_ms is not None:
                durations.append(last_qualifying_ms - start_ms)
            start_ms = last_qualifying_ms = None

        if qualifies(row):
            if start_ms is None:
                start_ms = timestamp_ms
            last_qualifying_ms = timestamp_ms
        elif start_ms is not None:
            durations.append(last_qualifying_ms - start_ms)
            start_ms = last_qualifying_ms = None
        previous_ms = timestamp_ms

    if start_ms is not None:
        durations.append(last_qualifying_ms - start_ms)
    return durations


def count_persistence_events(
    rows: list,
    *,
    midline_bps: float,
    fees_bps: float,
    bands=CANDIDATES,
    durations_sec=PERSISTENCE_SECONDS,
    max_gap_ms: int = MAX_SAMPLE_GAP_MS,
) -> dict:
    ordered = sorted(rows, key=lambda r: r["timestamp_ms"])
    result = {}
    for band in bands:
        sell_durations = _event_durations_ms(
            ordered,
            lambda r, band=band: r["sell_edge_bps"] - midline_bps - fees_bps >= band,
            max_gap_ms,
        )
        buy_durations = _event_durations_ms(
            ordered,
            lambda r, band=band: r["buy_edge_bps"] + midline_bps - fees_bps >= band,
            max_gap_ms,
        )
        result[band] = {
            "sell": {
                duration: sum(1 for value in sell_durations if value >= duration * 1000)
                for duration in durations_sec
            },
            "buy": {
                duration: sum(1 for value in buy_durations if value >= duration * 1000)
                for duration in durations_sec
            },
        }
    return result


def print_persistence_analysis(
    path: str, rows: list, midline_bps: float, fees_bps: float
) -> None:
    if not rows:
        print(f"\n=== {path}: persistence research ===")
        print("sample-data span: 0.0s")
        print("observed coverage: 0.0s")
        print("usable samples: 0")
        print("coverage source: samples CSV (independent of minute-data coverage)")
        return

    span_sec = (rows[-1]["timestamp_ms"] - rows[0]["timestamp_ms"]) / 1000.0
    observed_coverage_sec = (
        sum(
            current["timestamp_ms"] - previous["timestamp_ms"]
            for previous, current in zip(rows, rows[1:])
            if current["timestamp_ms"] - previous["timestamp_ms"] <= MAX_SAMPLE_GAP_MS
        )
        / 1000.0
    )
    print(f"\n=== {path}: persistence research ===")
    print(f"sample-data span: {span_sec:.1f}s")
    print(f"observed coverage: {observed_coverage_sec:.1f}s")
    print(f"usable samples: {len(rows)}")
    print("coverage source: samples CSV (independent of minute-data coverage)")
    print(f"gap rule: a gap > {MAX_SAMPLE_GAP_MS} ms ends the current event")
    print("Persistence event counts (not minute counts), by minimum duration:")
    labels = " ".join(f">={duration}s" for duration in PERSISTENCE_SECONDS)
    print(f"  {'band bps':>9} | {'direction':>13} | {labels}")

    counts = count_persistence_events(
        rows,
        midline_bps=midline_bps,
        fees_bps=fees_bps,
    )
    for band in CANDIDATES:
        for direction, label in (("sell", "SELL entropy"), ("buy", "BUY entropy")):
            values = " ".join(
                f"{counts[band][direction][duration]:>4}"
                for duration in PERSISTENCE_SECONDS
            )
            print(f"  {band:>9.1f} | {label:>13} | {values}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="suggest thresholds from recorded minute data"
    )
    p.add_argument("--csv", default="logs/minutes.csv")
    p.add_argument(
        "--hours",
        type=float,
        default=0.0,
        help="only use the last N hours (0 = all data)",
    )
    p.add_argument(
        "--min-samples",
        type=int,
        default=10,
        help="skip minutes with fewer fresh samples than this",
    )
    p.add_argument(
        "--fees-bps",
        type=float,
        default=0.0,
        help="SUM of both venues' taker fees in bps (each crossing "
        "pays both legs); recorded edges are pre-fee, so this "
        "is subtracted before counting firings (default 0.0 — "
        "pass ~1.0 with a tradexyz hedge)",
    )
    p.add_argument(
        "--samples",
        help="optional second-level samples CSV for persistence event analysis",
    )
    args = p.parse_args()

    try:
        rows = load_rows(args.csv, args.hours, args.min_samples)
    except FileNotFoundError:
        print(
            f"{args.csv} not found — run the bot (even --record-only) to "
            f"collect data first / 未找到数据文件，请先运行机器人采集数据",
            file=sys.stderr,
        )
        sys.exit(1)
    if len(rows) < 30:
        print(
            f"only {len(rows)} usable minute(s) in {args.csv} — collect at "
            f"least a few hours before trusting the numbers / 数据太少，"
            f"建议至少采集数小时",
            file=sys.stderr,
        )
        if not rows:
            sys.exit(1)

    span_h = (rows[-1]["ts"] - rows[0]["ts"]) / 3600.0 + 1 / 60.0
    prem = sorted(r["prem"] for r in rows)
    mean = sum(prem) / len(prem)
    var = sum((x - mean) ** 2 for x in prem) / len(prem)
    median = pctl(prem, 50)

    print(f"\n=== {args.csv}: {len(rows)} minutes over {span_h:.1f}h ===\n")
    print(
        "premium of Entropy over hedge, minute close (bps) / Entropy 相对对冲腿的溢价:"
    )
    print(f"  mean {mean:+.2f}   std {math.sqrt(var):.2f}   median {median:+.2f}")
    print(
        f"  p5 {pctl(prem, 5):+.2f}   p25 {pctl(prem, 25):+.2f}   "
        f"p75 {pctl(prem, 75):+.2f}   p95 {pctl(prem, 95):+.2f}"
    )

    midline = round(median, 1) or 0.0  # normalize -0.0
    # room beyond the midline that was actually executable each minute, net
    # of taker fees (config thresholds are net-of-fee: the engine adds fees
    # on top, and recorded edges are pre-fee)
    fees = args.fees_bps
    sell_room = sorted((r["sell_max"] - midline - fees for r in rows), reverse=True)
    buy_room = sorted((r["buy_max"] + midline - fees for r in rows), reverse=True)

    print(
        f"\nwith midline_bps = {midline:+.1f} (median) and {fees:.1f} bps "
        f"round-trip taker fees, minutes each band would have fired / "
        f"各档净阈值触发的分钟数:"
    )
    print(f"  {'band bps':>9} | {'SELL entropy':>17} | {'BUY entropy':>17}")
    print(f"  {'':>9} | {'minutes':>8} {'per day':>8} | {'minutes':>8} {'per day':>8}")
    per_day = 24.0 / span_h if span_h > 0 else 0.0
    for t in CANDIDATES:
        s_hits = sum(1 for x in sell_room if x >= t)
        b_hits = sum(1 for x in buy_room if x >= t)
        print(
            f"  {t:>9.1f} | {s_hits:>8} {s_hits * per_day:>8.1f} | "
            f"{b_hits:>8} {b_hits * per_day:>8.1f}"
        )

    # default suggestion: the band that fired in ~10% of minutes (p90 of the
    # fee-adjusted executable room), floored at 1 bps — tune from the table
    sug_upper = max(round(pctl(sorted(sell_room), 90) * 2) / 2, 1.0)
    sug_lower = max(round(pctl(sorted(buy_room), 90) * 2) / 2, 1.0)
    print(f"""
suggested starting point (fires ~10% of minutes, already net of the
{fees:.1f} bps fees passed via --fees-bps; a full round trip nets
>= upper+lower bps after fees) /
建议起点（约 10% 的分钟触发；已扣除 --fees-bps 传入的 {fees:.1f} bps 手续费，
一次完整往返扣费后净赚 >= upper+lower bps）:

thresholds:
  midline_bps: {midline}
  upper_bps: {sug_upper}
  lower_bps: {sug_lower}

Re-run with --hours to focus on recent regimes; premiums drift, so refresh
these numbers regularly. / 溢价中枢会漂移，请定期重新分析并更新配置。
""")

    if args.samples:
        try:
            sample_rows = load_sample_rows(args.samples, args.hours)
        except FileNotFoundError:
            print(
                f"{args.samples} not found — collect second-level sample data first",
                file=sys.stderr,
            )
            sys.exit(1)
        print_persistence_analysis(args.samples, sample_rows, midline, fees)


if __name__ == "__main__":
    main()
