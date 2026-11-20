"""Small synthetic fixtures shaped like the Milan files (never real Milan data)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from producer import integrity

# 2013-11-05 is a Milan local day starting 2013-11-04 23:00 UTC (CET, UTC+1).
DAY = "2013-11-05"
DAY_START_MS = int(datetime(2013, 11, 4, 23, 0, tzinfo=UTC).timestamp() * 1000)
STEP_MS = 600_000


def write_grid(path: Path, side: int = 100) -> None:
    """A side x side grid with Milan-like coordinates; id = row * side + col + 1."""
    feats = []
    for row in range(side):
        for col in range(side):
            lon0, lat0 = 9.0115 + col * 0.003, 45.3577 + row * 0.00211
            ring = [
                [lon0, lat0],
                [lon0 + 0.003, lat0],
                [lon0 + 0.003, lat0 + 0.00211],
                [lon0, lat0 + 0.00211],
                [lon0, lat0],
            ]
            feats.append(
                {
                    "type": "Feature",
                    "properties": {"cellId": row * side + col + 1},
                    "geometry": {"type": "Polygon", "coordinates": [ring]},
                }
            )
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))


def tsv_row(square: int, t_ms: int, cc: int, *vals: float | None) -> str:
    cells = ["" if v is None else repr(v) for v in vals]
    cells += [""] * (5 - len(cells))
    return "\t".join([str(square), str(t_ms), str(cc), *cells])


@pytest.fixture
def make_published() -> Callable[..., dict[str, integrity.PublishedFile]]:
    def _make(*paths: Path) -> dict[str, integrity.PublishedFile]:
        out = {}
        for p in paths:
            h = integrity.hash_file(p)
            out[p.name] = integrity.PublishedFile(p.name, "10.0/TEST", 1, h.md5, h.size_bytes)
        return out

    return _make
