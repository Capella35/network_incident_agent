from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from producer import grid, integrity, manifest
from tests.producer.conftest import write_grid


def test_grid_ids_and_block(tmp_path: Path) -> None:
    write_grid(tmp_path / "g.geojson")
    squares = grid.load_squares(tmp_path / "g.geojson")
    assert len(squares) == 10_000
    assert (squares[101].row, squares[101].col) == (1, 0)

    sub = grid.build_subset(tmp_path / "g.geojson")
    assert sub["size"] == 144
    ids = sub["square_ids"]
    assert ids == sorted(set(ids))
    assert sub["centre_square_id"] in ids
    rows = {(i - 1) // 100 for i in ids}
    cols = {(i - 1) % 100 for i in ids}
    assert len(rows) == len(cols) == 12  # contiguous 12 x 12 block
    assert max(rows) - min(rows) == max(cols) - min(cols) == 11


def test_subset_is_deterministic(tmp_path: Path) -> None:
    write_grid(tmp_path / "g.geojson")
    assert grid.build_subset(tmp_path / "g.geojson") == grid.build_subset(tmp_path / "g.geojson")


def test_block_outside_grid_rejected(tmp_path: Path) -> None:
    write_grid(tmp_path / "g.geojson", side=100)
    squares = grid.load_squares(tmp_path / "g.geojson")
    with pytest.raises(ValueError, match="outside"):
        grid.select_block(squares, squares[1], 12)


def test_published_checksums_cover_all_days() -> None:
    pub = integrity.load_published()
    days = [n for n in pub if n.startswith("sms-call-internet-mi-")]
    assert len(days) == 62
    assert pub["milano-grid.geojson"].md5 == "e64ed3858e97347c14cd049efde8c7d3"


def test_verify_accepts_match_and_rejects_tampering(
    tmp_path: Path, make_published: Callable[..., dict[str, integrity.PublishedFile]]
) -> None:
    f = tmp_path / "milano-grid.geojson"
    f.write_text("original")
    pub = make_published(f)
    integrity.verify(f, pub)

    f.write_text("tampered")  # same size, different content
    with pytest.raises(integrity.IntegrityError, match="MD5"):
        integrity.verify(f, pub)
    f.write_text("short")
    with pytest.raises(integrity.IntegrityError, match="size"):
        integrity.verify(f, pub)


def test_verify_rejects_unknown_file(tmp_path: Path) -> None:
    f = tmp_path / "unknown.txt"
    f.write_text("x")
    with pytest.raises(integrity.IntegrityError, match="not a known"):
        integrity.verify(f, {})


def test_manifest_roundtrip_and_upsert(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    doc = manifest.load(path)
    manifest.upsert(doc, "a.txt", {"md5": "1", "processed": {"rows_out": 3}})
    manifest.upsert(doc, "a.txt", {"md5": "2"})  # keeps `processed`
    manifest.save(path, doc)
    again = manifest.load(path)
    assert again["files"]["a.txt"] == {"md5": "2", "processed": {"rows_out": 3}}
    assert not list(tmp_path.glob(".manifest-*"))  # no temp files left


def test_manifest_rejects_unknown_version(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"version": 99, "files": {}}')
    with pytest.raises(ValueError, match="version"):
        manifest.load(path)
