from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from common import splits
from producer import data_prep, integrity
from tests.producer.conftest import DAY, DAY_START_MS, tsv_row, write_grid

Published = dict[str, integrity.PublishedFile]


@pytest.fixture
def data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_published: Callable[..., Published],
) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir()
    write_grid(raw / "milano-grid.geojson")
    day = raw / f"sms-call-internet-mi-{DAY}.txt"
    day.write_text(tsv_row(5061, DAY_START_MS, 39, 1.0, 2.0, 3.0, 4.0, 5.0) + "\n")
    pub = make_published(raw / "milano-grid.geojson", day)
    monkeypatch.setattr(integrity, "load_published", lambda: pub)
    return tmp_path


def test_run_writes_output_subset_and_manifest(data_dir: Path) -> None:
    assert data_prep.run(data_dir) == 1

    subset = json.loads((data_dir / "processed" / "subset.json").read_text())
    assert subset["size"] == 144

    out = data_dir / "processed" / "traffic" / f"{DAY}.parquet"
    frame = pl.read_parquet(out)
    assert frame.height == 144 * 144
    assert frame["data_gap"].sum() == 144 * 144 - 1

    doc = json.loads((data_dir / "manifest.json").read_text())
    entry = doc["files"][f"sms-call-internet-mi-{DAY}.txt"]
    assert entry["md5_matches_published"] is True
    assert entry["license"] == "ODbL-1.0"
    assert entry["processed"]["rows_out"] == 144 * 144
    assert entry["processed"]["output_sha256"] == integrity.sha256_file(out)
    assert "milano-grid.geojson" in doc["files"]


def test_rerun_is_idempotent_and_deterministic(data_dir: Path) -> None:
    data_prep.run(data_dir)
    out = data_dir / "processed" / "traffic" / f"{DAY}.parquet"
    first = integrity.sha256_file(out)

    paths = data_prep.Paths(data_dir)
    doc = json.loads(paths.manifest.read_text())
    subset = json.loads(paths.subset.read_text())["square_ids"]
    day = paths.raw / f"sms-call-internet-mi-{DAY}.txt"
    pub = integrity.load_published()
    assert data_prep.process_file(day, subset, paths, pub, doc) is False  # up to date
    assert data_prep.process_file(day, subset, paths, pub, doc, force=True) is True
    assert integrity.sha256_file(out) == first  # same bytes when reprocessed


def test_rerun_leaves_manifest_unchanged(data_dir: Path) -> None:
    data_prep.run(data_dir)
    before = (data_dir / "manifest.json").read_text()
    data_prep.run(data_dir)
    assert (data_dir / "manifest.json").read_text() == before


def test_tampered_file_is_rejected_and_not_processed(data_dir: Path) -> None:
    day = data_dir / "raw" / f"sms-call-internet-mi-{DAY}.txt"
    day.write_text(day.read_text().replace("5061", "5062"))
    assert data_prep.main(["--data-dir", str(data_dir)]) == 1
    assert not (data_dir / "processed" / "traffic").exists()


def test_delete_raw(data_dir: Path) -> None:
    data_prep.run(data_dir, delete_raw=True)
    assert not (data_dir / "raw" / f"sms-call-internet-mi-{DAY}.txt").exists()
    assert (data_dir / "raw" / "milano-grid.geojson").exists()  # grid is kept


def test_missing_requested_date(data_dir: Path) -> None:
    assert data_prep.main(["--data-dir", str(data_dir), "--date", "2013-12-01"]) == 1


def test_require_processed_days_names_the_fix(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"no processed validation days.*make data"):
        data_prep.require_processed_days(tmp_path, splits.Split.VALIDATION)
    paths = data_prep.Paths(tmp_path)
    paths.traffic.mkdir(parents=True)
    day = splits.days(splits.Split.VALIDATION)[0]
    paths.traffic_day(day).write_bytes(b"stub")
    assert list(data_prep.require_processed_days(tmp_path, splits.Split.VALIDATION)) == [day]
