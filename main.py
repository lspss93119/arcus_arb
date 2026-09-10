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
    record_only: bool, tiny_live: bool, confirm_mainnet: bool
) -> str:
    """Validate the explicit runtime mode gates before loading credentials.

    The two requested B0 flags are necessary but the first order has one
    additional, separate human-approval gate in ``main``.  Keeping this
    helper independent makes accidental live-by-default regressions easy to
    test without constructing an exchange client.
    """
    if record_only and tiny_live:
        raise ValueError("--record-only and --tiny-live are mutually exclusive")
    if confirm_mainnet and not tiny_live:
        raise ValueError("--confirm-mainnet requires --tiny-live")
    if tiny_live and not confirm_mainnet:
        raise ValueError(
            "--tiny-live requires the explicit --confirm-mainnet acknowledgement"
        )
    if record_only:
        return "record-only"
    if tiny_live:
        return "tiny-live"
    raise ValueError(
        "pass --record-only, or pass both --tiny-live and --confirm-mainnet"
    )


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
) -> None:
    eng = Engine(
        cfg,
        record_only=record_only,
        tiny_live=tiny_live,
        confirm_mainnet=confirm_mainnet,
        allow_first_order=approve_first_order,
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
        description="Arcus SNDK × Lighter-RH SNDK recorder and gated B0 "
        "maker-first calibration"
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
        "--confirm-mainnet",
        action="store_true",
        help="required acknowledgement for --tiny-live mainnet account access",
    )
    p.add_argument(
        "--approve-first-order",
        action="store_true",
        help="separate human approval gate; permits the first B0 "
        "mainnet ALO only after preflight (use deliberately)",
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
        validate_runtime_gates(args.record_only, args.tiny_live, args.confirm_mainnet)
    except ValueError as e:
        print(f"runtime mode error: {e}", file=sys.stderr)
        sys.exit(2)
    if args.approve_first_order and not args.tiny_live:
        print(
            "runtime mode error: --approve-first-order requires --tiny-live",
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
