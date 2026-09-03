"""Config loading: example file, validation, CLI-selected markets.

Run:  python3 -m pytest tests/  (or  python3 tests/test_config.py)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import ConfigError, load_config  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def write_tmp(text: str) -> str:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(text)
    f.close()
    return f.name


MINIMAL = """
strategy:
  name: stable_basis
  params:
    center_bps: 5.0
    upper_bps: 4.0
    lower_bps: 3.0
"""


def load(yaml_text: str, symbol="SNDK", hedge="lighter-rh"):
    return load_config(write_tmp(yaml_text), NO_ENV,
                       symbol=symbol, hedge_venue=hedge)


def test_example_config_loads():
    example_file = os.path.join(os.path.dirname(__file__), "..",
                                "config.example.yaml")
    cfg = load_config(example_file, NO_ENV,
                      symbol="SNDK", hedge_venue="lighter-rh")
    assert cfg.symbol == "SNDK"
    assert cfg.arcus.kind == "arcus" and cfg.arcus.label == "ARCUS"
    assert cfg.hedge_venue == "lighter-rh"
    assert cfg.hedge.kind == "lighter"
    assert cfg.hedge.lighter_profile.chain_id == 466324
    assert cfg.arcus.symbol == "SNDK" and cfg.hedge.symbol == "SNDK"
    assert cfg.recorder_enabled and cfg.recorder_database
    assert cfg.dashboard and cfg.log_file
    assert cfg.strategy.name == "stable_basis"
    assert cfg.strategy.center_bps == 0.0
    assert cfg.strategy.upper_bps == 4.0
    assert cfg.strategy.lower_bps == 4.0
    assert not hasattr(cfg, "midline_bps")
    assert not hasattr(cfg, "upper_bps")
    assert not hasattr(cfg, "lower_bps")


def test_minimal_defaults():
    cfg = load(MINIMAL, hedge="lighter")
    assert cfg.strategy.name == "stable_basis"
    assert cfg.strategy.center_bps == 5.0
    assert cfg.strategy.upper_bps == 4.0
    assert cfg.strategy.lower_bps == 3.0
    assert cfg.hedge.label == "LIGHTER"
    assert cfg.hedge.lighter_profile.chain_id == 304
    assert cfg.take_fraction == 0.5          # defaults kick in
    assert cfg.recorder_enabled is True
    assert cfg.recorder_database == "data/market-history.sqlite"


def test_stable_strategy_config_loads():
    cfg = load(MINIMAL)
    assert cfg.strategy.name == "stable_basis"
    assert cfg.strategy.center_bps == 5.0
    assert cfg.strategy.upper_bps == 4.0
    assert cfg.strategy.lower_bps == 3.0
    assert cfg.strategy.window_minutes is None
    assert cfg.strategy.center_mode == "fixed"


def test_stable_strategy_rolling_center_config_loads():
    cfg = load("""
strategy:
  name: stable_basis
  params:
    center_mode: rolling
    center_bps: -1.8
    center_window_hours: 12
    center_update_minutes: 60
    upper_bps: 0.75
    lower_bps: 0.75
""")
    assert cfg.strategy.center_mode == "rolling"
    assert cfg.strategy.center_bps == -1.8
    assert cfg.strategy.center_window_hours == 12
    assert cfg.strategy.center_update_minutes == 60


def test_rolling_center_parameters_are_strictly_validated():
    expect_error("""
strategy:
  name: stable_basis
  params:
    center_mode: adaptive
    center_bps: -1.8
    upper_bps: 0.75
    lower_bps: 0.75
""", "center_mode must be 'fixed' or 'rolling'")
    expect_error("""
strategy:
  name: stable_basis
  params:
    center_mode: rolling
    center_bps: -1.8
    center_window_hours: 0
    upper_bps: 0.75
    lower_bps: 0.75
""", "center_window_hours")
    expect_error("""
strategy:
  name: stable_basis
  params:
    center_mode: rolling
    center_bps: -1.8
    center_update_minutes: 0
    upper_bps: 0.75
    lower_bps: 0.75
""", "center_update_minutes")


def test_drifting_strategy_config_loads():
    cfg = load("""
strategy:
  name: drifting_basis
  params:
    window_minutes: 60
    upper_bps: 3.0
    lower_bps: 3.5
""")
    assert cfg.strategy.name == "drifting_basis"
    assert cfg.strategy.window_minutes == 60
    assert cfg.strategy.center_bps is None


def test_lighter_mainnet_anth_symbol_alias_preserves_canonical_symbol():
    cfg = load(MINIMAL, symbol="ANTH", hedge="lighter")

    assert cfg.symbol == "ANTH"
    assert cfg.entropy.symbol == "ANTH"
    assert cfg.hedge.symbol == "ANTHROPIC"
    assert cfg.recorder_database == "data/market-history.sqlite"


def test_lighter_rh_anth_symbol_alias_preserves_canonical_symbol():
    cfg = load(MINIMAL, symbol="ANTH", hedge="lighter-rh")

    assert cfg.symbol == "ANTH"
    assert cfg.entropy.symbol == "ANTH"
    assert cfg.hedge.symbol == "ANTHROPIC"
    assert cfg.recorder_database == "data/market-history.sqlite"


def test_recorder_database_defaults_and_is_shared():
    cfg_a = load(MINIMAL, symbol="SNDK", hedge="lighter-rh")
    cfg_b = load(MINIMAL, symbol="ANTH", hedge="lighter")
    assert cfg_a.recorder_enabled is True
    assert cfg_a.recorder_database == "data/market-history.sqlite"
    assert cfg_b.recorder_database == "data/market-history.sqlite"


def test_custom_recorder_database_is_preserved():
    cfg = load(
        MINIMAL + "\nrecorder:\n  database: archive/history.sqlite\n",
        symbol="SNDK",
        hedge="lighter-rh",
    )
    assert cfg.recorder_database == "archive/history.sqlite"


def test_legacy_recorder_csv_gets_actionable_error():
    expect_error(
        MINIMAL + "\nrecorder:\n  csv: logs/minutes.csv\n",
        "legacy 'recorder.csv' is no longer supported",
    )


def test_tradexyz_hedge():
    cfg = load(MINIMAL, hedge="tradexyz")
    assert cfg.hedge.kind == "hl" and cfg.hedge.hl_dex == "xyz"
    assert cfg.hedge.label == "XYZ"


def expect_error(yaml_text: str, needle: str, **kw):
    try:
        load(yaml_text, **kw)
    except ConfigError as e:
        assert needle in str(e), f"{needle!r} not in {e}"
        return
    raise AssertionError(f"expected ConfigError containing {needle!r}")


def test_unknown_key_rejected():
    expect_error(MINIMAL + "\nthresholdz:\n  x: 1\n",
                 "unknown config key 'thresholdz'")
    expect_error(MINIMAL + "\nsizing:\n  take_fractionn: 0.5\n",
                 "sizing.take_fractionn")


def test_unknown_strategy_rejected():
    expect_error("""
strategy:
  name: unknown_basis
  params:
    upper_bps: 3
    lower_bps: 3
""", "unknown strategy")


def test_strategy_specific_params_rejected():
    expect_error("""
strategy:
  name: drifting_basis
  params:
    center_bps: 1
    window_minutes: 60
    upper_bps: 3
    lower_bps: 3
""", "center_bps")
    expect_error("""
strategy:
  name: stable_basis
  params:
    center_bps: 1
    window_minutes: 60
    upper_bps: 3
    lower_bps: 3
""", "window_minutes")


def test_legacy_thresholds_get_actionable_migration_error():
    expect_error("""
thresholds:
  midline_bps: -1
  upper_bps: 3
  lower_bps: 3.5
""", "strategy:")


def test_nonfinite_strategy_values_rejected():
    expect_error("""
strategy:
  name: stable_basis
  params:
    center_bps: .nan
    upper_bps: 3
    lower_bps: 3
""", "finite")


def test_markets_no_longer_config_keys():
    # symbol / hedge_venue moved to --symbol / --hedge: leftovers in the
    # YAML must fail loudly, not silently override the flags
    expect_error("symbol: SNDK\n" + MINIMAL, "unknown config key 'symbol'")
    expect_error("hedge_venue: tradexyz\n" + MINIMAL,
                 "unknown config key 'hedge_venue'")


def test_bad_cli_markets():
    expect_error(MINIMAL, "--hedge", hedge="binance")
    expect_error(MINIMAL, "--symbol", symbol="")


def test_missing_thresholds():
    expect_error("recorder:\n  enabled: true\n", "strategy")


def test_nonpositive_band():
    expect_error("strategy:\n"
                 "  name: stable_basis\n"
                 "  params:\n"
                 "    center_bps: 5\n"
                 "    upper_bps: 0\n"
                 "    lower_bps: 3\n",
                 "must be > 0")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
