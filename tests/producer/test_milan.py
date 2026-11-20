from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from producer import milan
from tests.producer.conftest import DAY, DAY_START_MS, STEP_MS, tsv_row


def test_day_intervals_utc_and_count() -> None:
    iv = milan.day_intervals(date(2013, 11, 5))
    assert len(iv) == 144
    assert iv[0] == datetime(2013, 11, 4, 23, 0, tzinfo=UTC)
    assert iv[-1] == datetime(2013, 11, 5, 22, 50, tzinfo=UTC)


def test_day_intervals_dst_change() -> None:
    # 2013-10-27: Europe/Rome falls back, so the local day has 25 hours.
    assert len(milan.day_intervals(date(2013, 10, 27))) == 150


def test_file_date() -> None:
    assert milan.file_date(Path("sms-call-internet-mi-2013-12-02.txt")) == date(2013, 12, 2)
    with pytest.raises(ValueError):
        milan.file_date(Path("evil.txt"))


def _write(tmp_path: Path, rows: list[str]) -> Path:
    p = tmp_path / f"sms-call-internet-mi-{DAY}.txt"
    p.write_text("\n".join(rows) + "\n")
    return p


def test_process_day_sums_fills_and_flags(tmp_path: Path) -> None:
    t0, t1 = DAY_START_MS, DAY_START_MS + STEP_MS
    path = _write(
        tmp_path,
        [
            tsv_row(10, t0, 39, 1.0, 2.0, 3.0, 4.0, 5.0),
            tsv_row(10, t0, 0, 0.5, None, None, None, 1.5),  # second country code, empties
            tsv_row(10, t1, 39, None, None, None, None, 2.0),
            tsv_row(11, t0, 39, None, None, 1.0),
            tsv_row(99, t0, 39, 7.0, 7.0, 7.0, 7.0, 7.0),  # not in subset
            tsv_row(10, t0 - STEP_MS, 39, 9.0),  # previous local day
        ],
    )
    res = milan.process_day(path, [11, 10])
    f = res.frame

    assert f.height == 2 * 144
    assert f.columns == ["square_id", "interval_start", *milan.ACTIVITY_FIELDS, "data_gap"]
    assert str(f.schema["interval_start"]) == "Datetime(time_unit='ms', time_zone='UTC')"
    # sorted by (interval_start, square_id)
    assert f["square_id"][:4].to_list() == [10, 11, 10, 11]

    first = f.row(0, named=True)
    assert first["square_id"] == 10
    assert first["sms_in"] == pytest.approx(1.5)  # 1.0 + 0.5 across country codes
    assert first["sms_out"] == pytest.approx(2.0)  # empty counts as 0
    assert first["internet"] == pytest.approx(6.5)
    assert first["data_gap"] is False

    sq11_t1 = f.row(3, named=True)
    assert sq11_t1["square_id"] == 11
    assert sq11_t1["data_gap"] is True  # no source row
    assert sq11_t1["internet"] == 0.0

    assert res.rows_in == 6
    assert res.rows_subset == 5
    assert res.rows_out_of_day == 1
    assert res.gap_rows == 2 * 144 - 3


def test_row_with_all_fields_empty_is_not_a_gap(tmp_path: Path) -> None:
    # A source row with every activity field empty = recorded zero activity, not a data gap.
    path = _write(tmp_path, [tsv_row(10, DAY_START_MS, 39)])
    row = milan.process_day(path, [10]).frame.row(0, named=True)
    assert row["data_gap"] is False
    assert row["internet"] == 0.0


def test_misaligned_timestamp_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, [tsv_row(10, DAY_START_MS + 1, 39, 1.0)])
    with pytest.raises(ValueError, match="10-minute"):
        milan.process_day(path, [10])
