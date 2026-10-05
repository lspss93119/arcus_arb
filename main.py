#!/usr/bin/env python3
"""arcus-arb entry point.

    # collect public market data only — no credentials needed
    python3 main.py --record-only --symbol SNDK --hedge lighter-rh

--symbol and --hedge are required on every start: the markets you trade are
an explicit decision, not a config default. Phase A requires --record-only;
Phase B0 adds a separately gated, pre-order-only tiny-live calibration
workflow.  A further ``--approve-first-order`` flag is intentionally required
before that workflow can submit its first mainnet order.

On a terminal the bot shows a live Rich market-data dashboard (BBO, premium,
RTH state, sequence health, recorder rows, and raw L2 event count) and writes
log lines to logging.file; use --no-dashboard for plain console logs
(nohup/systemd). See the README (English) / README.zh-CN.md (中文).
"""

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys

from entropy_arb.config import ConfigError, load_config
from entropy_arb.engine import Engine


def validate_runtime_gates(
    record_only: bool,
    tiny_live: bool,
    confirm_mainnet: bool,
    *,
    volume_probe: bool = False,
    rolling_live: bool = False,
) -> str:
    """Validate the explicit runtime mode gates before loading credentials.

    The two requested B0 flags are necessary but the first order has one
    additional, separate human-approval gate in ``main``.  Keeping this
    helper independent makes accidental live-by-default regressions easy to
    test without constructing an exchange client.
    """
    selected = int(bool(record_only)) + int(bool(tiny_live)) + int(bool(volume_probe)) + int(bool(rolling_live))
    if selected > 1:
        raise ValueError(
            "--record-only, --tiny-live, --volume-probe, and --rolling-live are mutually exclusive"
        )
    if (volume_probe or rolling_live or tiny_live) and not confirm_mainnet:
        flag = "--rolling-live" if rolling_live else "--volume-probe" if volume_probe else "--tiny-live"
        raise ValueError(
            f"{flag} requires the explicit --confirm-mainnet acknowledgement"
        )
    if confirm_mainnet and not (tiny_live or volume_probe or rolling_live):
        raise ValueError(
            "--confirm-mainnet requires --tiny-live, --volume-probe, or --rolling-live"
        )
    if record_only:
        return "record-only"
    if tiny_live:
        return "tiny-live"
    if volume_probe:
        return "volume-probe"
    if rolling_live:
        return "rolling-live"
    raise ValueError("pass --record-only, or pass one live mode with --confirm-mainnet")


def setup_logging(
    level: str,
    log_file: str | None = None,
    extra_handler: logging.Handler | None = None,
) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if log_file:
        d = os.path.dirname(log_file)
        if d:
            os.makedirs(d, exist_ok=True)
        h: logging.Handler = logging.FileHandler(log_file)
    else:
        h = logging.StreamHandler()
    h.setFormatter(fmt)
    root.addHandler(h)
    if extra_handler is not None:
        root.addHandler(extra_handler)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def amain(
    cfg,
    record_only: bool,
    use_dashboard: bool,
    force_tty: bool,
    log_buffer,
    lang: str,
    *,
    tiny_live: bool = False,
    confirm_mainnet: bool = False,
    approve_first_order: bool = False,
    volume_probe: bool = False,
    rolling_live: bool = False,
    rolling_clip_usd: float = 1000.0,
    rolling_reprice_sec: float = 30.0,
    rolling_max_runtime_sec: int = 3600,
    rolling_max_loss_usd: float = 10.0,
    probe_clip_usd: float | None = None,
    probe_side: str | None = None,
    probe_reprice_sec: float = 30.0,
    probe_max_runtime_sec: int = 1800,
    probe_max_loss_usd: float = 10.0,
) -> None:
    eng = Engine(
        cfg,
        record_only=record_only,
        tiny_live=tiny_live,
        confirm_mainnet=confirm_mainnet,
        allow_first_order=approve_first_order,
        volume_probe=volume_probe,
        rolling_live=rolling_live,
        rolling_clip_usd=rolling_clip_usd,
        rolling_reprice_sec=rolling_reprice_sec,
        rolling_max_runtime_sec=rolling_max_runtime_sec,
        rolling_max_loss_usd=rolling_max_loss_usd,
        probe_clip_usd=probe_clip_usd,
        probe_side=probe_side,
        probe_reprice_sec=probe_reprice_sec,
        probe_max_runtime_sec=probe_max_runtime_sec,
        probe_max_loss_usd=probe_max_loss_usd,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, eng.request_stop)
    if not use_dashboard:
        await eng.run()
        return
    from entropy_arb.dashboard import Dashboard

    dash = Dashboard(eng, log_buffer, cfg.log_file, force_terminal=force_tty, lang=lang)
    dash_task = asyncio.create_task(dash.run(), name="dashboard")
    try:
        await eng.run()
    finally:
        eng.request_stop()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(dash_task, timeout=5)
        if not dash_task.done():
            dash_task.cancel()


def main() -> None:
    p = argparse.ArgumentParser(
        description="Arcus × Lighter-RH recorder, probes, and explicitly gated rolling canary"
    )
    p.add_argument(
        "--symbol",
        required=True,
        help="symbol traded on both venues, e.g. SNDK / 两个交易所共同交易的品种",
    )
    p.add_argument(
        "--hedge",
        required=True,
        choices=("lighter-rh",),
        metavar="VENUE",
        help="Phase A hedge venue: lighter-rh / 对冲腿：lighter-rh",
    )
    p.add_argument(
        "--config", default="config.yaml", help="strategy config (default: config.yaml)"
    )
    p.add_argument(
        "--env-file",
        default=".env",
        help="optional local env-file path; record-only ignores Arcus credentials",
    )
    p.add_argument(
        "--record-only",
        action="store_true",
        help="collect public BBO, trades, and raw L2 data only; "
        "run no strategy or orders (needs no credentials)",
    )
    p.add_argument(
        "--tiny-live",
        action="store_true",
        help="B0 preflight / explicitly gated tiny Arcus ALO maker "
        "calibration; never live-by-default",
    )
    p.add_argument(
        "--volume-probe",
        action="store_true",
        help="one-shot Arcus maker/RH hedge/unwind probe; independently gated",
    )
    p.add_argument(
        "--rolling-live",
        action="store_true",
        help="long-running research-selected rolling Arcus maker/RH taker canary; explicitly gated",
    )
    p.add_argument(
        "--confirm-mainnet",
        action="store_true",
        help="required acknowledgement for any live account-access mode",
    )
    p.add_argument(
        "--approve-first-order",
        action="store_true",
        help="separate human approval gate; permits the first B0, probe, or rolling "
        "mainnet ALO only after preflight (use deliberately)",
    )
    p.add_argument(
        "--rolling-clip-usd",
        type=float,
        default=1000.0,
        help="rolling canary Arcus maker clip in USD (default: 1000)",
    )
    p.add_argument(
        "--rolling-reprice-sec",
        type=float,
        default=30.0,
        help="rolling canary maker cancel/reprice interval (default: 30)",
    )
    p.add_argument(
        "--rolling-max-runtime-sec",
        type=int,
        default=3600,
        help="rolling canary hard runtime cap (default: 3600)",
    )
    p.add_argument(
        "--rolling-max-loss-usd",
        type=float,
        default=10.0,
        help="rolling canary hard realized-loss cap (default: 10)",
    )
    p.add_argument(
        "--probe-clip-usd",
        type=float,
        default=None,
        help="required approximate USD notional for one volume-probe clip",
    )
    p.add_argument(
        "--probe-side",
        choices=("buy", "sell"),
        default=None,
        help="required volume-probe build side on Arcus",
    )
    p.add_argument(
        "--probe-reprice-sec",
        type=float,
        default=30.0,
        help="volume-probe maker reprice interval (default: 30)",
    )
    p.add_argument(
        "--probe-max-runtime-sec",
        type=int,
        default=1800,
        help="volume-probe maximum runtime (default: 1800)",
    )
    p.add_argument(
        "--probe-max-loss-usd",
        type=float,
        default=10.0,
        help="volume-probe realized-loss cap (default: 10)",
    )
    p.add_argument(
        "--cn",
        action="store_true",
        help="display the dashboard in Chinese / 仪表盘使用中文",
    )
    disp = p.add_mutually_exclusive_group()
    disp.add_argument(
        "--dashboard",
        action="store_true",
        help="force the Rich dashboard even without a tty",
    )
    disp.add_argument(
        "--no-dashboard",
        action="store_true",
        help="plain console logs instead of the dashboard",
    )
    args = p.parse_args()

    try:
        validate_runtime_gates(
            args.record_only,
            args.tiny_live,
            args.confirm_mainnet,
            volume_probe=args.volume_probe,
            rolling_live=args.rolling_live,
        )
    except ValueError as e:
        print(f"runtime mode error: {e}", file=sys.stderr)
        sys.exit(2)
    if args.approve_first_order and not (
        args.tiny_live or args.volume_probe or args.rolling_live
    ):
        print(
            "runtime mode error: --approve-first-order requires --tiny-live, "
            "--volume-probe, or --rolling-live",
            file=sys.stderr,
        )
        sys.exit(2)
    if not args.volume_probe and (
        args.probe_clip_usd is not None or args.probe_side is not None
    ):
        print(
            "runtime mode error: --probe-clip-usd and --probe-side require "
            "--volume-probe",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.rolling_live:
        if args.rolling_clip_usd <= 0:
            print(
                "runtime mode error: --rolling-clip-usd must be > 0", file=sys.stderr
            )
            sys.exit(2)
        if args.rolling_reprice_sec <= 0:
            print(
                "runtime mode error: --rolling-reprice-sec must be > 0", file=sys.stderr
            )
            sys.exit(2)
        if args.rolling_max_runtime_sec <= 0:
            print(
                "runtime mode error: --rolling-max-runtime-sec must be > 0",
                file=sys.stderr,
            )
            sys.exit(2)
        if args.rolling_max_loss_usd <= 0:
            print(
                "runtime mode error: --rolling-max-loss-usd must be > 0",
                file=sys.stderr,
            )
            sys.exit(2)

    if args.volume_probe:
        if args.probe_clip_usd is None or args.probe_clip_usd <= 0:
            print(
                "runtime mode error: --volume-probe requires --probe-clip-usd > 0",
                file=sys.stderr,
            )
            sys.exit(2)
        if args.probe_side is None:
            print(
                "runtime mode error: --volume-probe requires --probe-side",
                file=sys.stderr,
            )
            sys.exit(2)
        if args.probe_reprice_sec <= 0:
            print(
                "runtime mode error: --probe-reprice-sec must be > 0", file=sys.stderr
            )
            sys.exit(2)
        if args.probe_max_runtime_sec <= 0:
            print(
                "runtime mode error: --probe-max-runtime-sec must be > 0",
                file=sys.stderr,
            )
            sys.exit(2)
        if args.probe_max_loss_usd <= 0:
            print(
                "runtime mode error: --probe-max-loss-usd must be > 0",
                file=sys.stderr,
            )
            sys.exit(2)

    try:
        cfg = load_config(
            args.config, args.env_file, symbol=args.symbol, hedge_venue=args.hedge
        )
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(2)

    use_dashboard = (cfg.dashboard or args.dashboard) and not args.no_dashboard
    force_tty = args.dashboard
    if use_dashboard and not (sys.stdout.isatty() or force_tty):
        use_dashboard = False

    log_buffer = None
    if use_dashboard:
        try:
            from entropy_arb.dashboard import BufferLogHandler
        except ImportError:
            print(
                "`rich` is not installed — falling back to plain logs "
                "(pip install -r requirements.txt)",
                file=sys.stderr,
            )
            use_dashboard = False
    if use_dashboard:
        log_buffer = BufferLogHandler()
        setup_logging(cfg.log_level, log_file=cfg.log_file, extra_handler=log_buffer)
    else:
        setup_logging(cfg.log_level)

    try:
        asyncio.run(
            amain(
                cfg,
                record_only=args.record_only,
                tiny_live=args.tiny_live,
                confirm_mainnet=args.confirm_mainnet,
                approve_first_order=args.approve_first_order,
                volume_probe=args.volume_probe,
                rolling_live=args.rolling_live,
                rolling_clip_usd=args.rolling_clip_usd,
                rolling_reprice_sec=args.rolling_reprice_sec,
                rolling_max_runtime_sec=args.rolling_max_runtime_sec,
                rolling_max_loss_usd=args.rolling_max_loss_usd,
                probe_clip_usd=args.probe_clip_usd,
                probe_side=args.probe_side,
                probe_reprice_sec=args.probe_reprice_sec,
                probe_max_runtime_sec=args.probe_max_runtime_sec,
                probe_max_loss_usd=args.probe_max_loss_usd,
                use_dashboard=use_dashboard,
                force_tty=force_tty,
                log_buffer=log_buffer,
                lang="zh" if args.cn else "en",
            )
        )
    except RuntimeError as e:
        # startup failures (missing credentials, market not found, venue
        # unreachable) — a clean message, not a traceback
        print(f"startup error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
