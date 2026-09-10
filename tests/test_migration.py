import csv
import sqlite3
from pathlib import Path

from entropy_arb.migration import migrate_directory

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


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def report_for(reports, filename: str):
    return next(report for report in reports if Path(report.path).name == filename)


def sample_row(ts=1_700_000_000_000, premium=10.0):
    return [ts, premium, 8.0, -12.0, 100.09, 100.11, 99.99, 100.01, ts - 500, ts - 300]


def minute_row(ts=1_699_999_980):
    return [
        ts,
        "2023-11-14T22:13:00Z",
        "SNDK",
        "lighter-rh",
        100.09,
        100.11,
        99.99,
        100.01,
        10.0,
        20.0,
        10.0,
        20.0,
        15.0,
        5.0,
        13.0,
        18.0,
        -17.0,
        -12.0,
        2,
    ]


def test_valid_samples_minutes_and_reference_import(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    write_csv(src / "samples-v2-SNDK-lighter-rh.csv", SAMPLE_HEADER, [sample_row()])
    write_csv(src / "minutes-SNDK-lighter-rh.csv", MINUTE_HEADER, [minute_row()])
    write_csv(
        src / "reference-SNDK-lighter-rh-entropy.csv",
        ENTROPY_REFERENCE_HEADER,
        [[1_700_000_000_100, 100.0, 100.1]],
    )
    write_csv(
        src / "reference-SNDK-lighter-rh.csv",
        HEDGE_REFERENCE_HEADER,
        [[1_700_000_000_200, 1_700_000_000_150, 99.9, 100.0]],
    )

    reports = migrate_directory(src, database, {})

    assert {report.status for report in reports} == {"PASS"}
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM samples").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM minutes").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM entropy_reference").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM hedge_reference").fetchone() == (1,)


def test_rerun_is_idempotent(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2-SNDK-lighter-rh.csv"
    write_csv(src / name, SAMPLE_HEADER, [sample_row()])

    first = report_for(migrate_directory(src, database, {}), name)
    second = report_for(migrate_directory(src, database, {}), name)

    assert first.inserted_rows == 1
    assert second.inserted_rows == 0
    assert second.already_existing == 1
    assert second.status == "PASS"


def test_duplicate_rows_are_counted(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2-SNDK-lighter-rh.csv"
    row = sample_row()
    write_csv(src / name, SAMPLE_HEADER, [row, row])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.source_rows == 2
    assert report.valid_rows == 2
    assert report.inserted_rows == 1
    assert report.already_existing == 1
    assert report.conflicting_key_rows == 0
    assert report.status == "PASS"


def test_same_key_different_payload_is_conflict_and_does_not_replace(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2-SNDK-lighter-rh.csv"
    write_csv(
        src / name, SAMPLE_HEADER, [sample_row(premium=10.0), sample_row(premium=999.0)]
    )

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.inserted_rows == 1
    assert report.conflicting_key_rows == 1
    assert report.status == "PARTIAL"
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT premium_bps FROM samples").fetchone() == (10.0,)


def test_invalid_numeric_row_is_counted_and_valid_rows_still_import(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2-SNDK-lighter-rh.csv"
    bad = sample_row(ts=1_700_000_000_001)
    bad[4] = "not-a-price"
    write_csv(src / name, SAMPLE_HEADER, [sample_row(), bad])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.source_rows == 2
    assert report.valid_rows == 1
    assert report.invalid_rows == 1
    assert report.inserted_rows == 1
    assert report.status == "PARTIAL"


def test_all_invalid_rows_fail_without_recording_a_migration_timestamp(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2-SNDK-lighter-rh.csv"
    bad = sample_row()
    bad[4] = "not-a-price"
    write_csv(src / name, SAMPLE_HEADER, [bad])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.status == "FAIL"
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT value FROM meta WHERE key='last_migration_at_utc'"
            ).fetchone()
            is None
        )


def test_unknown_schema_fails_closed(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2-SNDK-lighter-rh.csv"
    write_csv(src / name, ["timestamp", "price"], [[1, 100]])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.status == "FAIL"
    assert report.inserted_rows == 0


def test_unknown_sample_schema_fails_before_provenance_resolution(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2.csv"
    write_csv(src / name, ["timestamp", "price"], [[1, 100]])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.status == "FAIL"
    assert report.inserted_rows == 0


def test_ambiguous_samples_without_companion_needs_mapping(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2.csv"
    write_csv(src / name, SAMPLE_HEADER, [sample_row()])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.status == "NEEDS_MAPPING"
    assert report.inserted_rows == 0


def test_malformed_companion_minutes_is_unresolved_without_crashing(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2.csv"
    write_csv(src / name, SAMPLE_HEADER, [sample_row()])
    write_csv(src / "minutes.csv", MINUTE_HEADER, [[1_699_999_980]])

    report = report_for(migrate_directory(src, database, {}), name)

    assert report.status == "NEEDS_MAPPING"
    assert report.inserted_rows == 0


def test_database_open_failure_with_no_candidates_reports_fail(tmp_path: Path):
    source = tmp_path / "empty-logs"
    source.mkdir()
    database = tmp_path / "not-a-sqlite-database"
    database.write_text("not sqlite")

    reports = migrate_directory(source, database, {})

    assert len(reports) == 1
    assert reports[0].status == "FAIL"


def test_explicit_samples_mapping_succeeds(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    name = "samples-v2.csv"
    write_csv(src / name, SAMPLE_HEADER, [sample_row()])

    reports = migrate_directory(src, database, {name: ("SNDK", "lighter-rh")})
    report = report_for(reports, name)

    assert report.status == "PASS"
    assert report.inserted_rows == 1
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT symbol,hedge FROM samples").fetchone() == (
            "SNDK",
            "lighter-rh",
        )


def test_original_csv_bytes_and_mtime_are_unchanged(tmp_path: Path):
    src = tmp_path / "logs"
    src.mkdir()
    database = tmp_path / "history.sqlite"
    path = src / "samples-v2-SNDK-lighter-rh.csv"
    write_csv(path, SAMPLE_HEADER, [sample_row()])
    before_bytes = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns

    migrate_directory(src, database, {})

    assert path.read_bytes() == before_bytes
    assert path.stat().st_mtime_ns == before_mtime
